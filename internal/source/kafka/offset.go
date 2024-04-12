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
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// OffsetSeeker finds offsets within Kafka topics.
type OffsetSeeker interface {
	// GetOffsets finds the most recent offsets for resolved timestamp messages
	// that are before the given time, and in the given topics.
	GetOffsets([]string, hlc.Time) ([]*partitionState, error)
	// Close shuts down the connection with the Kafka broker.
	Close() error
}

// offsetSeeker implements the OffsetSeeker interface.
type offsetSeeker struct {
	client                 sarama.Client
	consumer               sarama.Consumer
	resolvedIntervalMillis int64
}

// NewOffsetSeeker instantiates an offsetManager.
func NewOffsetSeeker(config *Config) (OffsetSeeker, error) {
	cl, err := sarama.NewClient(config.Brokers, config.saramaConfig)
	if err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumerFromClient(cl)
	if err != nil {
		return nil, err
	}
	return &offsetSeeker{
		client:                 cl,
		consumer:               consumer,
		resolvedIntervalMillis: config.ResolvedInterval.Milliseconds(),
	}, nil
}

var _ OffsetSeeker = &offsetSeeker{}

// GetOffsets implements OffsetSeeker.
func (o *offsetSeeker) GetOffsets(topics []string, min hlc.Time) ([]*partitionState, error) {
	res := make([]*partitionState, 0)
	// TODO (silvano): make this parallel
	for _, topic := range topics {
		partitions, err := o.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			offset, err := o.getPartitionOffset(min, topic, partition)
			if err != nil {
				return nil, err
			}
			res = append(res, &partitionState{
				topic:     topic,
				partition: partition,
				offset:    offset,
			})
		}
	}
	return res, nil
}

// Close implements OffsetSeeker
func (o *offsetSeeker) Close() error {
	if err := o.consumer.Close(); err != nil {
		return err
	}
	return o.client.Close()
}

// getPartitionOffset get the most recent offsets at the given time
// for a specific topic and partition.
func (o *offsetSeeker) getPartitionOffset(
	min hlc.Time, topic string, partition int32,
) (int64, error) {
	minMillis := min.Nanos() / int64(time.Millisecond)
	// Get the offset at log head.
	loghead, err := o.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	log.Debugf("loghead for %s@%d = %d", topic, partition, loghead)
	last := loghead
	var offset int64
	// Looking for a offset that is reasonably just before the given min
	// resolved timestamp.
	// We want to limit as much as possible applying messages that are before
	// the given min timestamp, but we want to make sure we don't miss any
	// mutation after it.
	for {
		// Get the most recent available offset at the given time.
		offset, err = o.client.GetOffset(topic, partition, minMillis)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		// We are all caught up
		if offset == sarama.OffsetNewest {
			return sarama.OffsetNewest, nil
		}
		// If the topic has low traffic, we make sure we go back at least
		// one message since the last message we saw.
		if offset >= last {
			offset = last - 1
		}
		if offset < 0 {
			// There are no messages in the channel older than the
			// min timestamp.
			offset = sarama.OffsetOldest
			break
		}
		max := last
		last = offset
		// Verify that we see the min timestamp right after the offset.
		offset, err = o.seekResolved(min, topic, partition,
			offsetRange{offset, max})
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if offset != sarama.OffsetOldest {
			break
		}
		// If we get sarama.OffsetOldest, we try to see if can do better by
		// skipping back resolvedIntervalMillis.
		minMillis = minMillis - o.resolvedIntervalMillis
	}
	log.Infof("topic:%s partition:%d offset:%d latest:%d", topic, partition, offset, loghead)
	return offset, nil
}

// seekResolved finds the most recent resolved timestamp message within the
// specified offset range that is before the given time.
func (o *offsetSeeker) seekResolved(
	min hlc.Time, topic string, partition int32, offsets offsetRange,
) (int64, error) {
	log.Debugf("seekResolved: finding a message earlier than %s within [%d - %d]", min, offsets.min, offsets.max)
	partConsumer, err := o.consumer.ConsumePartition(topic, partition, offsets.min)
	if err != nil {
		return 0, err
	}
	defer partConsumer.Close()
	found := sarama.OffsetOldest
	for {
		select {
		case msg, ok := <-partConsumer.Messages():
			seekMessagesCount.WithLabelValues(topic, strconv.Itoa(int(partition))).Inc()
			if !ok {
				return 0, errors.Errorf("consumer for %s closed", topic)
			}
			if msg.Offset >= offsets.max {
				return found, nil
			}
			payload, err := asPayload(msg)
			if err != nil {
				return 0, err
			}
			if payload.Resolved != "" {
				timestamp, err := hlc.Parse(payload.Resolved)
				if err != nil {
					return 0, err
				}
				log.Debugf("seekResolved: checking message @%d %d", msg.Offset, timestamp.Nanos())
				if hlc.Compare(min, timestamp) <= 0 {
					if found >= 0 {
						log.Debugf("seekResolved: found message @%d", found)
					}
					// we are seeing a resolved message after the min timestamp
					// the previous found value is what we are looking for.
					return found, nil
				}
				found = msg.Offset
			}
		case err := <-partConsumer.Errors():
			return 0, err
		}
	}
}
