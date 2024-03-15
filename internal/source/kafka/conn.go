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
//
//	note: we get resolved timestamps on all the partitions,
//	      so we should be able to leverage that.
//
// TODO (silvano): support Avro format, schema registry.
// TODO (silvano): add metrics.
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
func (c *Conn) Start(ctx *stopper.Context) error {
	version, err := sarama.ParseKafkaVersion(c.config.version)
	if err != nil {
		return err
	}
	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer is initialized.
	 */
	c.saramaConfig = sarama.NewConfig()
	c.saramaConfig.Version = version

	switch c.config.strategy {
	case "sticky":
		c.saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		c.saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		c.saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		log.Panicf("Unrecognized consumer rebalance strategy: %s", c.config.strategy)
	}

	if c.config.oldest {
		c.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	c.group, err = sarama.NewConsumerGroup(c.config.brokers, c.config.group, c.saramaConfig)
	if err != nil {
		return errors.Wrap(err, "error creating consumer group client")
	}
	var start []*partitionState
	if c.config.from != "" {
		start, err = c.getOffsets(c.config.timeRange.Min().Nanos())
		if err != nil {
			return errors.Wrap(err, "cannot get offsets")
		}
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
				log.WithError(err).Warn("error while copying messages; will retry")
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
			offset, err := client.GetOffset(topic, partition, int64(nanos/1000000))
			if err != nil {
				return nil, err
			}
			log.Debugf("offset for %s@%d = %d", topic, partition, offset)
			res = append(res, &partitionState{
				topic:     topic,
				partition: partition,
				offset:    offset,
			})

		}
	}
	return res, nil
}
