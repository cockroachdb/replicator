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
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
// TODO (silvano): support Avro format, schema registry.
// https://github.com/cockroachdb/cdc-sink/issues/776
type Conn struct {
	// The connector configuration.
	config *Config
	// Delivers mutation to the target database.
	conveyor Conveyor
	// The group id used when connecting to the broker.
	group sarama.ConsumerGroup
	// The handler that processes the events.
	handler sarama.ConsumerGroupHandler
}

type offsetRange struct {
	min int64
	max int64
}

// Start the replication loop. Connect to the Kafka cluster and process events
// from the given topics.
// If more that one processes is started, the partitions within the topics
// are allocated to each process based on the chosen rebalance strategy.
func (c *Conn) Start(ctx *stopper.Context) (err error) {
	var start []*partitionState
	if c.config.MinTimestamp != "" {
		start, err = c.getOffsets(c.config.timeRange.Min())
		if err != nil {
			return errors.Wrap(err, "cannot get offsets")
		}
	}
	c.group, err = sarama.NewConsumerGroup(c.config.Brokers, c.config.Group, c.config.saramaConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	c.handler = &Handler{
		conveyor:  c.conveyor,
		batchSize: c.config.BatchSize,
		schema:    c.config.TargetSchema,
		timeRange: c.config.timeRange,
		fromState: start,
	}

	// Start a process to copy data to the target.
	ctx.Go(func() error {
		defer c.group.Close()
		for !ctx.IsStopping() {
			if err := c.copyMessages(ctx); err != nil {
				log.WithError(err).Error("error while copying messages; will retry")
				select {
				case <-ctx.Stopping():
				case <-time.After(time.Second):
				}
			}
		}
		return nil
	})
	return nil
}

// copyMessages is the main replication loop. It will open a connection
// to the source, accumulate messages, and commit data to the target.
func (c *Conn) copyMessages(ctx *stopper.Context) error {
	return c.group.Consume(ctx, c.config.Topics, c.handler)
}

// getOffsets finds the offsets based on resolved timestamp messages
func (c *Conn) getOffsets(min hlc.Time) ([]*partitionState, error) {
	seeker, err := NewOffsetSeeker(c.config)
	if err != nil {
		return nil, err
	}
	defer seeker.Close()
	return seeker.GetOffsets(c.config.Topics, min)
}
