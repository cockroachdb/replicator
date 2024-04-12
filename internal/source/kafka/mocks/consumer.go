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

package mocks

import "github.com/IBM/sarama"

// MockConsumer is a consumer for a fixed set of messages.
type MockConsumer struct {
	oldestOffset int64
	messages     map[string]map[int32][]*Message
}

var _ sarama.Consumer = &MockConsumer{}

// NewMockConsumer creates a consumer for the given set of messages
func NewMockConsumer(messages map[string]map[int32][]*Message, oldestOffset int64) *MockConsumer {
	return &MockConsumer{
		messages:     messages,
		oldestOffset: oldestOffset,
	}
}

// Close implements sarama.Consumer.
func (m *MockConsumer) Close() error {
	return nil
}

// ConsumePartition implements sarama.Consumer.
func (m *MockConsumer) ConsumePartition(
	topic string, partition int32, offset int64,
) (sarama.PartitionConsumer, error) {
	return NewMockPartitionConsumer(topic, partition, m.messages[topic][partition], offset, m.oldestOffset), nil
}

// HighWaterMarks implements sarama.Consumer.
func (m *MockConsumer) HighWaterMarks() map[string]map[int32]int64 {
	res := make(map[string]map[int32]int64)
	for topic, topicMessages := range m.messages {
		res[topic] = make(map[int32]int64)
		for part, partMessages := range topicMessages {
			res[topic][part] = int64(len(partMessages)) + m.oldestOffset
		}
	}
	return res
}

// Partitions implements sarama.Consumer.
func (m *MockConsumer) Partitions(topic string) ([]int32, error) {
	res := make([]int32, len(m.messages[topic]))
	i := 0
	for part := range m.messages[topic] {
		res[i] = part
		i++
	}
	return res, nil
}

// Pause implements sarama.Consumer.
func (m *MockConsumer) Pause(topicPartitions map[string][]int32) {
	panic("unimplemented")
}

// PauseAll implements sarama.Consumer.
func (m *MockConsumer) PauseAll() {
	panic("unimplemented")
}

// Resume implements sarama.Consumer.
func (m *MockConsumer) Resume(topicPartitions map[string][]int32) {
	panic("unimplemented")
}

// ResumeAll implements sarama.Consumer.
func (m *MockConsumer) ResumeAll() {
	panic("unimplemented")
}

// Topics implements sarama.Consumer.
func (m *MockConsumer) Topics() ([]string, error) {
	res := make([]string, len(m.messages))
	i := 0
	for topic := range m.messages {
		res[i] = topic
		i++
	}
	return res, nil
}
