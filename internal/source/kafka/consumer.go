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

type partitionBatch struct {
	data *types.MultiBatch
	keys map[string]hlc.Time
}

func newPartitionBatch() *partitionBatch {
	return &partitionBatch{
		data: &types.MultiBatch{},
		keys: make(map[string]hlc.Time),
	}
}

func (b *partitionBatch) Count() int {
	return b.data.Count()
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
	log.Infof("ConsumeClaim topic=%s partition=%d offset=%d", claim.Topic(), claim.Partition(), claim.InitialOffset())
	// Aggregate the mutations by target table.
	batch := newPartitionBatch()
	lastResolved := hlc.Zero()
	// Track last message received for each topic/partition.
	consumed := make(map[string]*sarama.ConsumerMessage)
	ctx := session.Context()
	partition := topicPartitionID(claim.Topic(), claim.Partition())
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
			payload, err := c.accumulate(batch, lastResolved, message)
			if err != nil {
				log.WithError(err).Error("failed to add messages to a batch")
				return err
			}
			if payload == nil {
				continue
			}
			if payload.Resolved != "" {

				timestamp, err := hlc.Parse(payload.Resolved)
				if err != nil {
					return err
				}
				lastResolved = timestamp
				log.Tracef("Resolved partition=%s  timestamp=%s", partition, timestamp)
				if hlc.Compare(timestamp, c.timeRange.Max()) > 0 {
					log.Infof("Done with topic=%s partition=%d  %+v", claim.Topic(), claim.Partition(), ctx)
					c.done(partition, true)
					return nil
				}
				if batch, err = c.accept(ctx, batch); err != nil {
					log.WithError(err).Error("failed to accept a batch")
					return err
				}
				if err := c.conveyor.Advance(ctx, ident.New(partition), timestamp); err != nil {
					return err
				}
				consumed[partition] = message
				c.mark(session, consumed)
				continue
			}
			consumed[partition] = message
			// Flush a batch, and mark the latest message for each topic/partition as read.
			if batch.Count() > c.batchSize {
				if batch, err = c.accept(ctx, batch); err != nil {
					log.WithError(err).Error("failed to accept a batch")
					return err
				}
				c.mark(session, consumed)
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			// Periodically flush a batch, and mark the latest message for each topic/partition as consumed.
			if batch, err = c.accept(ctx, batch); err != nil {
				log.WithError(err).Error("failed to accept a batch")
				return err
			}
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
func (c *Consumer) accept(ctx context.Context, batch *partitionBatch) (*partitionBatch, error) {
	if batch.Count() == 0 {
		// Nothing to do.
		return batch, nil
	}
	log.Debugf("flushing %d", batch.Count())
	if err := c.conveyor.AcceptMultiBatch(ctx, batch.data, &types.AcceptOptions{}); err != nil {
		return newPartitionBatch(), err
	}
	return newPartitionBatch(), nil
}

// accumulate adds the message to the batch and returns the decoded payload.
func (c *Consumer) accumulate(
	batch *partitionBatch, lastResolved hlc.Time, msg *sarama.ConsumerMessage,
) (*payload, error) {
	payload, err := asPayload(msg)
	if err != nil {
		return nil, err
	}
	if payload.Resolved != "" {
		log.Debugf("Resolved [%s@%d %d] %s ",
			msg.Topic, msg.Partition, msg.Offset, payload.Resolved)
		return payload, nil
	}
	log.Debugf("Mutation [%s@%d offset=%d time=%s] [key=%s mvcc=%s]",
		msg.Topic, msg.Partition, msg.Offset, msg.Timestamp, string(msg.Key), payload.Updated)
	timestamp, err := hlc.Parse(payload.Updated)
	if err != nil {
		return nil, err
	}
	// Derive table name from topic.
	table, qual, err := ident.ParseTableRelative(msg.Topic, c.schema.Schema())
	if err != nil {
		return nil, err
	}
	// Ensure the destination table is in the target schema.
	if qual != ident.TableOnly {
		table = ident.NewTable(c.schema.Schema(), table.Table())
	}
	// Discard mutations that are older that the last resolved timestamp seen.
	if hlc.Compare(timestamp, lastResolved) < 0 {
		log.Warnf("timestamp after before last resolved for key %s (%s < %s)", msg.Key, timestamp, lastResolved)
		return nil, nil
	}
	// Keep the most recent mutation for a specific key within a batch.
	key := string(msg.Key)
	if seen, ok := batch.keys[key]; ok && hlc.Compare(seen, timestamp) >= 0 {
		log.Debugf("skipping duplicate %s@%s", string(msg.Key), timestamp)
		return nil, nil
	}
	batch.keys[key] = timestamp
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
	return payload, batch.data.Accumulate(table, mut)
}
