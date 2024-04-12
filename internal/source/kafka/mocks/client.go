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

// Package mocks implements a simple Kafka client and consumer for testing purposes.
// The main use case is to test a consumer that has a predefined set of messages.
// Note: only the methods that are actively used for testing are implemented.
package mocks

import (
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

// MockClient implements sarama.Client interface for testing purposes.
type MockClient struct {
	closed   bool
	config   *sarama.Config
	consumer *MockConsumer
	offsets  map[string]map[int32][]time.Time
}

var _ sarama.Client = &MockClient{}

// NewMockClient instantiate a new client that always returns the given Consumer
func NewMockClient(config *sarama.Config, consumer *MockConsumer) *MockClient {
	offsets := make(map[string]map[int32][]time.Time)
	for topic, partitions := range consumer.messages {
		offsets[topic] = make(map[int32][]time.Time)
		for partition, messages := range partitions {
			offsets[topic][partition] = make([]time.Time, len(messages))
			for offset, message := range messages {
				offsets[topic][partition][offset] = message.Timestamp
			}
		}
	}
	return &MockClient{
		config:   config,
		consumer: consumer,
		offsets:  offsets,
	}
}

// Consumer returns the underlying sarama.Consumer.
func (m *MockClient) Consumer() sarama.Consumer {
	return m.consumer
}

// Broker implements sarama.Client.
func (m *MockClient) Broker(brokerID int32) (*sarama.Broker, error) {
	panic("unimplemented")
}

// Brokers implements sarama.Client.
func (m *MockClient) Brokers() []*sarama.Broker {
	panic("unimplemented")
}

// Close implements sarama.Client.
func (m *MockClient) Close() error {
	m.closed = true
	return m.consumer.Close()
}

// Closed implements sarama.Client.
func (m *MockClient) Closed() bool {
	return m.closed
}

// Config implements sarama.Client.
func (m *MockClient) Config() *sarama.Config {
	return m.config
}

// Controller implements sarama.Client.
func (m *MockClient) Controller() (*sarama.Broker, error) {
	panic("unimplemented")
}

// Coordinator implements sarama.Client.
func (m *MockClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	panic("unimplemented")
}

// GetOffset implements sarama.Client.
func (m *MockClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	offsets := m.offsets[topic][partitionID]
	if time == sarama.OffsetNewest {
		return int64(len(offsets)) + m.consumer.oldestOffset, nil
	}
	if time == sarama.OffsetOldest {
		return m.consumer.oldestOffset, nil
	}
	res := sarama.OffsetOldest
	for i, o := range offsets {
		log.Tracef("Comparing %d %d\n", o.UnixMilli(), time)
		if o.UnixMilli() > time {
			log.Debugf("Found %d %d\n", o.UnixMilli(), res)
			return res, nil
		}
		res = int64(i) + m.consumer.oldestOffset
	}
	return sarama.OffsetNewest, nil
}

// InSyncReplicas implements sarama.Client.
func (m *MockClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	panic("unimplemented")
}

// InitProducerID implements sarama.Client.
func (m *MockClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	panic("unimplemented")
}

// Leader implements sarama.Client.
func (m *MockClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	panic("unimplemented")
}

// LeaderAndEpoch implements sarama.Client.
func (m *MockClient) LeaderAndEpoch(
	topic string, partitionID int32,
) (*sarama.Broker, int32, error) {
	panic("unimplemented")
}

// LeastLoadedBroker implements sarama.Client.
func (m *MockClient) LeastLoadedBroker() *sarama.Broker {
	panic("unimplemented")
}

// OfflineReplicas implements sarama.Client.
func (m *MockClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	panic("unimplemented")
}

// Partitions implements sarama.Client.
func (m *MockClient) Partitions(topic string) ([]int32, error) {
	return m.consumer.Partitions(topic)
}

// RefreshBrokers implements sarama.Client.
func (m *MockClient) RefreshBrokers(addrs []string) error {
	panic("unimplemented")
}

// RefreshController implements sarama.Client.
func (m *MockClient) RefreshController() (*sarama.Broker, error) {
	panic("unimplemented")
}

// RefreshCoordinator implements sarama.Client.
func (m *MockClient) RefreshCoordinator(consumerGroup string) error {
	panic("unimplemented")
}

// RefreshMetadata implements sarama.Client.
func (m *MockClient) RefreshMetadata(topics ...string) error {
	panic("unimplemented")
}

// RefreshTransactionCoordinator implements sarama.Client.
func (m *MockClient) RefreshTransactionCoordinator(transactionID string) error {
	panic("unimplemented")
}

// Replicas implements sarama.Client.
func (m *MockClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	panic("unimplemented")
}

// Topics implements sarama.Client.
func (m *MockClient) Topics() ([]string, error) {
	panic("unimplemented")
}

// TransactionCoordinator implements sarama.Client.
func (m *MockClient) TransactionCoordinator(transactionID string) (*sarama.Broker, error) {
	panic("unimplemented")
}

// WritablePartitions implements sarama.Client.
func (m *MockClient) WritablePartitions(topic string) ([]int32, error) {
	panic("unimplemented")
}
