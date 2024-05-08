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
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/replicator/internal/source/kafka/mocks"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/stretchr/testify/assert"
)

func newTime(sec int64) hlc.Time {
	return hlc.New(sec*int64(time.Second), 0)
}
func timestamp(t hlc.Time) time.Time {
	return time.Unix(0, t.Nanos()+int64(rand.IntN(int(time.Millisecond))))
}
func resolved(t hlc.Time) *mocks.Message {
	return &mocks.Message{
		Timestamp: timestamp(t),
		Key:       []byte(""),
		Value:     []byte(fmt.Sprintf(`{"resolved":"%s"}`, t)),
	}
}
func mut(t hlc.Time, key string) *mocks.Message {
	return &mocks.Message{
		Timestamp: timestamp(t),
		Key:       []byte(key),
		Value:     []byte(fmt.Sprintf(`{"updated":"%s"}`, t)),
	}
}

func TestGetOffsets(t *testing.T) {
	tests := []struct {
		name         string
		topics       []string
		messages     map[string]map[int32][]*mocks.Message
		min          hlc.Time
		oldestOffset int64
		want         map[string]map[int]int64
		wantErr      bool
	}{
		{
			name:   "one topic, one empty partition",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {},
				},
			},
			min: newTime(4),
			want: map[string]map[int]int64{
				"topic": {
					0: sarama.OffsetNewest,
				},
			},
		},
		{
			name:   "one topic, one partition",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min: newTime(4),
			want: map[string]map[int]int64{
				"topic": {
					0: 3,
				},
			},
		},
		{
			name:   "one topic, one partition, oldest offset > 0",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min:          newTime(4),
			oldestOffset: 100,
			want: map[string]map[int]int64{
				"topic": {
					0: 103,
				},
			},
		},
		{
			name:   "one topic, one partition, replay",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {
						resolved(newTime(1)),
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min: newTime(4),
			want: map[string]map[int]int64{
				"topic": {
					0: 5,
				},
			},
		},
		{
			name:   "one topic, one partition from beginning",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min: newTime(0),
			want: map[string]map[int]int64{
				"topic": {
					0: sarama.OffsetOldest,
				},
			},
		},
		{
			name:   "one topic, one partition from beginning, oldest offset > 0",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min:          newTime(0),
			oldestOffset: 123,
			want: map[string]map[int]int64{
				"topic": {
					0: sarama.OffsetOldest,
				},
			},
		},
		{
			name:   "one topic, one partition, no pending messages",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min: newTime(6),
			want: map[string]map[int]int64{
				"topic": {
					0: sarama.OffsetNewest,
				},
			},
		},
		{
			name:   "one topic, multiple partitions",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"topic": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
					1: {
						resolved(newTime(1)),
						mut(newTime(2), "12"),
						resolved(newTime(2)),
						mut(newTime(3), "13"),
						resolved(newTime(3)),
						mut(newTime(4), "12"),
						mut(newTime(4), "13"),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
					2: {
						resolved(newTime(1)),
						mut(newTime(2), "20"),
						mut(newTime(2), "21"),
						mut(newTime(2), "22"),
						mut(newTime(2), "23"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						mut(newTime(4), "23"),
						mut(newTime(4), "22"),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min: newTime(4),
			want: map[string]map[int]int64{
				"topic": {
					0: 3,
					1: 4,
					2: 6,
				},
			},
		},
		{
			name:   "two topics, multiple partitions",
			topics: []string{"topic"},
			messages: map[string]map[int32][]*mocks.Message{
				"one": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						mut(newTime(3), "2"),
						resolved(newTime(3)),
						mut(newTime(4), "3"),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
				"two": {
					0: {
						resolved(newTime(1)),
						mut(newTime(2), "1"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
					1: {
						resolved(newTime(1)),
						mut(newTime(2), "12"),
						resolved(newTime(2)),
						mut(newTime(3), "13"),
						resolved(newTime(3)),
						mut(newTime(4), "12"),
						mut(newTime(4), "13"),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
					2: {
						resolved(newTime(1)),
						mut(newTime(2), "20"),
						mut(newTime(2), "21"),
						mut(newTime(2), "22"),
						mut(newTime(2), "23"),
						resolved(newTime(2)),
						resolved(newTime(3)),
						mut(newTime(4), "23"),
						mut(newTime(4), "22"),
						resolved(newTime(4)),
						resolved(newTime(5)),
					},
				},
			},
			min: newTime(4),
			want: map[string]map[int]int64{
				"one": {
					0: 4,
				},
				"two": {
					0: 3,
					1: 4,
					2: 6,
				},
			},
		},
	}
	intervals := []int64{500, 1000, 2000}
	for _, tt := range tests {
		for _, interval := range intervals {
			interval := interval
			name := fmt.Sprintf("%s_%d", tt.name, interval)
			t.Run(name, func(t *testing.T) {
				a := assert.New(t)
				consumer := mocks.NewMockConsumer(tt.messages, tt.oldestOffset)
				seeker := &offsetSeeker{
					client:                 mocks.NewMockClient(&sarama.Config{}, consumer),
					consumer:               consumer,
					resolvedIntervalMillis: interval,
				}
				got, err := seeker.GetOffsets(tt.topics, tt.min)
				a.NoError(err)
				for _, g := range got {
					a.Equal(tt.want[g.topic][int(g.partition)], g.offset)
				}
			})
		}
	}
}
