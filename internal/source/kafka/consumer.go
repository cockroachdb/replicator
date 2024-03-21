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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var _ sarama.ConsumerGroupHandler = &Handler{}

type partitionState struct {
	topic     string
	partition int32
	offset    int64
}

// Handler represents a Sarama consumer group consumer
type Handler struct {
	// The destination for writes.
	acceptor  types.MultiAcceptor
	batchSize int
	target    ident.Schema
	watchers  types.Watchers
	timeRange hlc.Range
	fromState []*partitionState
}

type payload struct {
	After    json.RawMessage `json:"after"`
	Before   json.RawMessage `json:"before"`
	Resolved string          `json:"resolved"`
	Updated  string          `json:"updated"`
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Handler) Setup(session sarama.ConsumerGroupSession) error {
	// If the startup option provide a minTimestamp we mark the offset to the provided
	// timestamp or the latest read message, whichever is later, for each topic and partition.
	// In case we restart the process, we are able to resume from the latest committed message
	// without changing the start up command.
	// TODO (silvano): Should we have a --force option to restart from the provided minTimestamp?
	//                 Using a different group id would have the same effect.
	for _, marker := range c.fromState {
		log.Debugf("setup: marking offset %s@%d to %d", marker.topic, marker.partition, marker.offset)
		session.MarkOffset(marker.topic, marker.partition, marker.offset, "start")
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Handler) Cleanup(session sarama.ConsumerGroupSession) error {
	if session.Context().Err() != nil {
		log.WithError(session.Context().Err()).Error("Session terminated with an error")
	}
	return session.Context().Err()
}

// ConsumeClaim processes new messages for the topic/partition specified in the claim.
func (c *Handler) ConsumeClaim(
	session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) (err error) {
	log.Debugf("ConsumeClaim topic=%s partition=%d offset=%d", claim.Topic(), claim.Partition(), claim.InitialOffset())
	// Aggregate the mutations by target table.
	toProcess := &types.MultiBatch{}
	// Track last message received for each topic/partition.
	consumed := make(map[string]*sarama.ConsumerMessage)
	ctx := session.Context()
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
			if err = c.accumulate(toProcess, message); err != nil {
				return err
			}
			consumed[fmt.Sprintf("%s@%d", message.Topic, message.Partition)] = message
			// Flush a batch, and mark the latest message for each topic/partition as read.
			if toProcess.Count() > c.batchSize {
				if err = c.accept(ctx, toProcess); err != nil {
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
				return err
			}
			toProcess = toProcess.Empty()
			c.mark(session, consumed)
		}
	}
}

// mark advances the offset on each topic/partition and removes it from the map that
// track the latest message received on the topic/partition.
func (c *Handler) mark(
	session sarama.ConsumerGroupSession, consumed map[string]*sarama.ConsumerMessage,
) {
	for key, message := range consumed {
		session.MarkMessage(message, "")
		delete(consumed, key)
	}
}

// accept process a batch.
func (c *Handler) accept(ctx context.Context, toProcess *types.MultiBatch) error {
	if toProcess.Count() == 0 {
		// Nothing to do.
		return nil
	}
	log.Tracef("flushing %d", toProcess.Count())
	if err := c.acceptor.AcceptMultiBatch(ctx, toProcess, &types.AcceptOptions{}); err != nil {
		return err
	}
	return nil
}

// accumulate adds the message to the batch, after converting it to a types.Mutation.
// Resolved messages are skipped.
func (c *Handler) accumulate(toProcess *types.MultiBatch, msg *sarama.ConsumerMessage) error {
	var payload payload
	dec := json.NewDecoder(bytes.NewReader(msg.Value))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		// Empty input is a no-op.
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errors.Wrap(err, "could not decode payload")
	}
	if payload.Resolved != "" {
		log.Tracef("Resolved %s %d [%s@%d]", payload.Resolved, msg.Timestamp.Unix(), msg.Topic, msg.Partition)
		return nil
	}
	log.Tracef("Mutation %s %d [%s@%d]", string(msg.Key), msg.Timestamp.Unix(), msg.Topic, msg.Partition)
	timestamp, err := hlc.Parse(payload.Updated)
	if err != nil {
		return err
	}
	table, qual, err := ident.ParseTableRelative(msg.Topic, c.target.Schema())
	if err != nil {
		return err
	}
	// Ensure the destination table is in the target schema.
	if qual != ident.TableOnly {
		table = ident.NewTable(c.target.Schema(), table.Table())
	}
	if !c.timeRange.Contains(timestamp) {
		log.Debugf("skipping mutation %s %s %s", string(msg.Key), timestamp, c.timeRange)
		return nil
	}
	mut := types.Mutation{
		Before: payload.Before,
		Data:   payload.After,
		Key:    msg.Key,
		Time:   timestamp,
	}
	script.AddMeta("kafka", table, &mut)
	log.Debugf("adding mutation %s", string(msg.Key))
	return toProcess.Accumulate(table, mut)
}
