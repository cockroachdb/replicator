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

import (
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

type Message struct {
	Timestamp  time.Time
	Key, Value []byte
}
type MockPartitionConsumer struct {
	done         chan struct{}
	errChan      chan *sarama.ConsumerError
	oldestOffset int64
	messages     []*Message
	msgChan      chan *sarama.ConsumerMessage
	offset       int64
	partition    int32
	topic        string
}

var _ sarama.PartitionConsumer = &MockPartitionConsumer{}

func NewMockPartitionConsumer(
	topic string, partition int32, messages []*Message, offset int64, oldestOffset int64,
) *MockPartitionConsumer {
	part := &MockPartitionConsumer{
		done:         make(chan struct{}),
		errChan:      make(chan *sarama.ConsumerError),
		messages:     messages,
		oldestOffset: oldestOffset,
		msgChan:      make(chan *sarama.ConsumerMessage),
		offset:       offset - oldestOffset,
		partition:    partition,
		topic:        topic,
	}
	go part.start()
	return part
}

func (m *MockPartitionConsumer) start() {
outer:
	for m.offset < int64(len(m.messages)) {
		msg := &sarama.ConsumerMessage{
			Key:       m.messages[m.offset].Key,
			Offset:    m.offset + m.oldestOffset,
			Timestamp: m.messages[m.offset].Timestamp,
			Topic:     m.topic,
			Value:     m.messages[m.offset].Value,
		}
		select {
		case <-m.done:
			break outer
		case m.msgChan <- msg:
			m.offset++
		}
	}
	log.Debug("MockPartitionConsumer is done")
	close(m.msgChan)
	close(m.errChan)
}

// AsyncClose implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) AsyncClose() {
	log.Debug("MockPartitionConsumer.AsyncClose")
	m.done <- struct{}{}
}

// Close implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) Close() error {
	m.AsyncClose()
	return nil
}

// Errors implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return m.errChan
}

// HighWaterMarkOffset implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) HighWaterMarkOffset() int64 {
	return int64(len(m.messages))
}

// IsPaused implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) IsPaused() bool {
	panic("unimplemented")
}

// Messages implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return m.msgChan
}

// Pause implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) Pause() {
	panic("unimplemented")
}

// Resume implements sarama.PartitionConsumer.
func (m *MockPartitionConsumer) Resume() {
	panic("unimplemented")
}
