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
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAccumulate verifies that we correctly add incoming consumer messages
// to a types.MultiBatch.
func TestAccumulate(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	tests := []struct {
		name    string
		msg     *sarama.ConsumerMessage
		wantErr string
	}{
		{
			name: "before",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 0,
				Key:       []byte(`[0]`),
				Value:     []byte(`{"after": {"k":0, "v": "a"}, "updated":"1.0"}`),
			},
		},
		{
			name: "insert",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 1,
				Key:       []byte(`[1]`),
				Value:     []byte(`{"after": {"k":1, "v": "a"}, "updated":"10.0"}`),
			},
		},
		{
			name: "update",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 1,
				Key:       []byte(`[1]`),
				Value:     []byte(`{"after": {"k":1, "v": "a"}, "before": {"k":1, "v": "b"},"updated":"11.0"}`),
			},
		},
		{
			name: "resolved partition 0",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Partition: 0,
				Topic:     "table",
				Value:     []byte(`{"resolved":"11.0"}`),
			},
		},
		{
			name: "resolved partition 1",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Partition: 1,
				Topic:     "table",
				Value:     []byte(`{"resolved":"11.0"}`),
			},
		},
		{
			name: "resolved partition 2",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Partition: 2,
				Topic:     "table",
				Value:     []byte(`{"resolved":"11.0"}`),
			},
		},
		{
			name: "delete",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 1,
				Key:       []byte(`[1]`),
				Value:     []byte(`{"before": {"k":1, "v": "b"}, "updated":"13.0"}`),
			},
		},
		{
			name: "after",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 2,
				Key:       []byte(`[2]`),
				Value:     []byte(`{"after": {"k":2, "v": "b"}, "updated":"20.0"}`),
			},
		},
		{
			name: "invalid",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Partition: 2,
				Topic:     "table",
				Value:     []byte(`{"invalid":"11.0"}`),
			},
			wantErr: "unknown field",
		},
		{
			name: "no topic",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Partition: 2,
				Key:       []byte(`[2]`),
				Value:     []byte(`{"after": {"k":2, "v": "b"}, "updated":"11.0"}`),
			},
			wantErr: "empty table name",
		},
		{
			name: "invalid time",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 2,
				Key:       []byte(`[2]`),
				Value:     []byte(`{"after": {"k":2, "v": "b"}, "updated":"test.0"}`),
			},
			wantErr: "invalid syntax",
		},
	}
	toProcess := &types.MultiBatch{}
	consumer := &Handler{
		timeRange: hlc.RangeIncluding(hlc.New(10, 0), hlc.New(13, 0)),
		target:    ident.MustSchema(ident.New("db"), ident.New("public")),
	}
	for _, test := range tests {
		err := consumer.accumulate(toProcess, test.msg)
		if test.wantErr != "" {
			a.Error(err)
			a.ErrorContains(err, test.wantErr)
		} else {
			r.NoError(err)
		}
	}
	// Verify the we accumulated all the messages within the time range.
	a.Equal(3, len(toProcess.Data))
	r.NotNil(toProcess.ByTime[hlc.New(10, 0)])
	a.Equal(1, toProcess.ByTime[hlc.New(10, 0)].Count())
	r.NotNil(toProcess.ByTime[hlc.New(11, 0)])
	a.Equal(1, toProcess.ByTime[hlc.New(11, 0)].Count())
	a.Nil(toProcess.ByTime[hlc.New(12, 0)])
	r.NotNil(toProcess.ByTime[hlc.New(13, 0)])
	a.Equal(1, toProcess.ByTime[hlc.New(13, 0)].Count())
	a.Nil(toProcess.ByTime[hlc.New(20, 0)])
}
