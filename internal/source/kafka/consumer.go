// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
)

var _ sarama.ConsumerGroupHandler = &Consumer{}

type partitionState struct {
	topic     string
	partition int32
	offset    int64
}

// Consumer represents a Kafka consumer
type Consumer struct {
	batchSize int               // Batch size for writes.
	conveyor  Conveyor          // The destination for writes.
	fromState []*partitionState // The initial offsets for each partitions.
	schema    ident.Schema      // The target schema.
	timeRange hlc.Range         // The time range for incoming mutations.
	mu        struct {
		sync.Mutex
		done map[string]bool
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	// If the startup option provide a minTimestamp we mark the offset
	// to the provided timestamp or the latest read message, whichever
	// is later, for each topic and partition. In case we restart the
	// process, we are able to resume from the latest committed message
	// without changing the start up command.
	//
	// TODO (silvano): Should we have a --force option to restart from
	// the provided minTimestamp? Using a different group id would have
	// the same effect.
	for _, marker := range c.fromState {
		log.Debugf("setup: marking offset %s@%d to %d", marker.topic, marker.partition, marker.offset)
		session.MarkOffset(marker.topic, marker.partition, marker.offset, "start")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.done = make(map[string]bool)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim
// goroutines have exited
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	if session.Context().Err() != nil {
		log.WithError(session.Context().Err()).Error("Session terminated with an error")
	}
	if c.allDone() {
		return nil
	}
	return session.Context().Err()
}

// ConsumeClaim processes new messages for the topic/partition specified in the claim.
func (c *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) (err error) {
	log.Debugf("ConsumeClaim topic=%s partition=%d offset=%d", claim.Topic(), claim.Partition(), claim.InitialOffset())
	// Aggregate the mutations by target table.
	toProcess := &types.MultiBatch{}
	// Track last message received for each topic/partition.
	consumed := make(map[string]*sarama.ConsumerMessage)
	ctx := session.Context()
	partition := fmt.Sprintf("%s@%d", claim.Topic(), claim.Partition())
	c.done(partition, false)
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Debugf("message channel for topic=%s partition=%d was closed", claim.Topic(), claim.Partition())
				return nil
			}
			payload, err := c.accumulate(toProcess, message)
			if err != nil {
				log.WithError(err).Error("failed to add messages to a batch")
				return err
			}
			if payload.Resolved != "" {
				partition := fmt.Sprintf("%s@%d", message.Topic, int(message.Partition))
				timestamp, err := hlc.Parse(payload.Resolved)
				if err != nil {
					return err
				}
				log.Tracef("Resolved partition=%s  timestamp=%s", partition, timestamp)
				if hlc.Compare(timestamp, c.timeRange.Max()) > 0 {
					log.Infof("Done with topic=%s partition=%d  %+v", claim.Topic(), claim.Partition(), ctx)
					c.done(partition, true)
					return nil
				}

				if err := c.conveyor.Advance(ctx, ident.New(partition), timestamp); err != nil {
					return err
				}
				if err := c.accept(ctx, toProcess); err != nil {
					log.WithError(err).Error("failed to accept a batch")
					return err
				}
				toProcess = toProcess.Empty()
				consumed[fmt.Sprintf("%s@%d", message.Topic, message.Partition)] = message
				c.mark(session, consumed)
				continue

			}
			consumed[fmt.Sprintf("%s@%d", message.Topic, message.Partition)] = message
			// Flush a batch, and mark the latest message for each topic/partition as read.
			if toProcess.Count() > c.batchSize {
				if err = c.accept(ctx, toProcess); err != nil {
					log.WithError(err).Error("failed to accept a batch")
					return err
				}
				toProcess = toProcess.Empty()
				c.mark(session, consumed)
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			// Periodically flush a batch, and mark the latest message for each topic/partition as consumed.
			if err = c.accept(ctx, toProcess); err != nil {
				log.WithError(err).Error("failed to accept a batch")
				return err
			}
			toProcess = toProcess.Empty()
			c.mark(session, consumed)
		}
	}
}

// allDone returns true if we processed all the messages before the
// maxTimestamp on all the partitions.
func (c *Consumer) allDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.mu.done {
		if !v {
			return false
		}
	}
	return true
}

// done set the state of a partition.
func (c *Consumer) done(partition string, done bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.done[partition] = done
}

// mark advances the offset on each topic/partition and removes it from the map that
// track the latest message received on the topic/partition.
func (c *Consumer) mark(
	session sarama.ConsumerGroupSession, consumed map[string]*sarama.ConsumerMessage,
) {
	for key, message := range consumed {
		session.MarkMessage(message, "")
		delete(consumed, key)
	}
}

// accept process a batch.
func (c *Consumer) accept(ctx context.Context, toProcess *types.MultiBatch) error {
	if toProcess.Count() == 0 {
		// Nothing to do.
		return nil
	}
	log.Tracef("flushing %d", toProcess.Count())
	if err := c.conveyor.AcceptMultiBatch(ctx, toProcess, &types.AcceptOptions{}); err != nil {
		return err
	}
	return nil
}

// accumulate adds the message to the batch and returns the decoded payload.
func (c *Consumer) accumulate(
	toProcess *types.MultiBatch, msg *sarama.ConsumerMessage,
) (*payload, error) {
	payload, err := asPayload(msg)
	if err != nil {
		return nil, err
	}
	if payload.Resolved != "" {
		log.Infof("Resolved [%s@%d %d] %s ",
			msg.Topic, msg.Partition, msg.Offset, payload.Resolved)
		return payload, nil
	}
	log.Infof("Mutation [%s@%d %d] %s %s",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), payload.Updated)
	timestamp, err := hlc.Parse(payload.Updated)
	if err != nil {
		return nil, err
	}
	table, qual, err := ident.ParseTableRelative(msg.Topic, c.schema.Schema())
	if err != nil {
		return nil, err
	}
	// Ensure the destination table is in the target schema.
	if qual != ident.TableOnly {
		table = ident.NewTable(c.schema.Schema(), table.Table())
	}
	if !c.timeRange.Contains(timestamp) {
		log.Debugf("skipping mutation %s %s %s", string(msg.Key), timestamp, c.timeRange)
		return nil, nil
	}
	mut := types.Mutation{
		Before: payload.Before,
		Data:   payload.After,
		Key:    msg.Key,
		Time:   timestamp,
	}
	script.AddMeta("kafka", table, &mut)
	log.Debugf("adding mutation %s", string(msg.Key))
	return payload, toProcess.Accumulate(table, mut)
}
