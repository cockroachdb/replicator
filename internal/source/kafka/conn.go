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
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
// TODO (silvano) : support transactional mode
// https://github.com/cockroachdb/cdc-sink/issues/777
// note: we get resolved timestamps on all the partitions,
// so we should be able to leverage that.
//
// TODO (silvano): support Avro format, schema registry.
// https://github.com/cockroachdb/cdc-sink/issues/776
//
// TODO (silvano): add metrics.
// https://github.com/cockroachdb/cdc-sink/issues/778
type Conn struct {
	// The destination for writes.
	acceptor types.MultiAcceptor
	// The connector configuration.
	config *Config

	// The kafka connector configuration.
	saramaConfig *sarama.Config
	// The group id used when connecting to the broker.
	group sarama.ConsumerGroup
	// The handler that processes the events.
	handler sarama.ConsumerGroupHandler
	// Access to the target database.
	targetDB *types.TargetPool
	// Access to the target schema.
	watchers types.Watchers
}

// Start the replication loop. Connect to the Kafka cluster and process events
// from the given topics.
// If more that one processes is started, the partitions within the topics
// are allocated to each process based on the chosen rebalance strategy.
func (c *Conn) Start(ctx *stopper.Context) (err error) {
	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer is initialized.
	 */
	c.saramaConfig = sarama.NewConfig()
	c.saramaConfig.Consumer.Group.Rebalance.GroupStrategies = c.config.rebalanceStrategy

	// Start from the oldest timestamp available in Kafka.
	// If the user provided a minTimestamp, it will be used instead.
	c.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	var start []*partitionState
	if c.config.minTimestamp != "" {
		start, err = c.getOffsets(c.config.timeRange.Min().Nanos())
		if err != nil {
			return errors.Wrap(err, "cannot get offsets")
		}
	}
	c.group, err = sarama.NewConsumerGroup(c.config.brokers, c.config.group, c.saramaConfig)
	if err != nil {
		return errors.Wrap(err, "error creating consumer group client")
	}

	c.handler = &Handler{
		acceptor:  c.acceptor,
		batchSize: c.config.batchSize,
		target:    c.config.TargetSchema,
		watchers:  c.watchers,
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
	return c.group.Consume(ctx, c.config.topics, c.handler)
}

// getOffsets get the most recent offsets at the given time
// for all the topics and partitions.
// TODO (silvano) : add testing
// https://github.com/cockroachdb/cdc-sink/issues/779
func (c *Conn) getOffsets(nanos int64) ([]*partitionState, error) {
	res := make([]*partitionState, 0)
	client, err := sarama.NewClient(c.config.brokers, c.saramaConfig)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	for _, topic := range c.config.topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			// Get the offset at log head.
			loghead, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}
			// Get the most recent available offset at the given time.
			offset, err := client.GetOffset(topic, partition, int64(nanos/1000000))
			if err != nil {
				return nil, err
			}
			// Use log head offset if there isn't a message at or after the given time.
			if offset == sarama.OffsetNewest {
				offset = loghead
			}
			log.Debugf("offset for %s@%d = %d (latest %d)", topic, partition, offset, loghead)
			res = append(res, &partitionState{
				topic:     topic,
				partition: partition,
				offset:    offset,
			})

		}
	}
	return res, nil
}
