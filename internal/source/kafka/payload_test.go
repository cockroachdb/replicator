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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAsPayload verifies that we can parse a Kafka message to a payload struct.
func TestAsPayload(t *testing.T) {
	tests := []struct {
		name    string
		msg     *sarama.ConsumerMessage
		want    *Payload
		wantErr string
	}{
		{
			name: "insert",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 0,
				Key:       []byte(`[0]`),
				Value:     []byte(`{"after": {"k":0, "v": "a"}, "updated":"1.0"}`),
			},
			want: &Payload{
				After:    []byte(`{"k":0, "v": "a"}`),
				Before:   nil,
				Key:      []byte(`[0]`),
				Resolved: "",
				Updated:  "1.0",
			},
		},
		{
			name: "insert",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 0,
				Key:       []byte(`[0]`),
				Value:     []byte(`{"after": {"k":0, "v": "a"}, "updated":"1.0"}`),
			},
			want: &Payload{
				After:    []byte(`{"k":0, "v": "a"}`),
				Before:   nil,
				Key:      []byte(`[0]`),
				Resolved: "",
				Updated:  "1.0",
			},
		},
		{
			name: "update",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 0,
				Key:       []byte(`[0]`),
				Value:     []byte(`{"after": {"k":0, "v": "b"},"before": {"k":0, "v": "a"}, "updated":"2.0"}`),
			},
			want: &Payload{
				After:    []byte(`{"k":0, "v": "b"}`),
				Before:   []byte(`{"k":0, "v": "a"}`),
				Key:      []byte(`[0]`),
				Resolved: "",
				Updated:  "2.0",
			},
		},
		{
			name: "delete",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 0,
				Key:       []byte(`[0]`),
				Value:     []byte(`{"before": {"k":0, "v": "a"}, "updated":"5.0"}`),
			},
			want: &Payload{
				After:    nil,
				Before:   []byte(`{"k":0, "v": "a"}`),
				Key:      []byte(`[0]`),
				Resolved: "",
				Updated:  "5.0",
			},
		},
		{
			name: "resolved",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 0,
				Key:       []byte(`[0]`),
				Value:     []byte(`{"resolved":"11.0"}`),
			},
			want: &Payload{
				After:    nil,
				Before:   nil,
				Key:      []byte(`[0]`),
				Resolved: "11.0",
				Updated:  "",
			},
		},
		{
			name: "invalid",
			msg: &sarama.ConsumerMessage{
				Timestamp: time.Now(),
				Topic:     "table",
				Partition: 0,
				Key:       []byte(`[0]`),
				Value:     []byte(`tt{"resolved":"11.0"}`),
			},
			wantErr: "could not decode payload",
		},
	}
	for _, tt := range tests {
		a := assert.New(t)
		r := require.New(t)
		decoder := &jsonDecoder{}
		got, err := decoder.Decode(tt.msg)
		if tt.wantErr != "" {
			a.Error(err)
			a.ErrorContains(err, tt.wantErr)
		} else {
			r.NoError(err)
		}
		a.Equal(tt.want, got)
	}

}
