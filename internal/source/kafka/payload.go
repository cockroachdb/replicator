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
	"bytes"
	"encoding/json"
	"io"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

// payload is the encoding of a mutation in a Kafka message.
type payload struct {
	After    json.RawMessage `json:"after"`
	Before   json.RawMessage `json:"before"`
	Resolved string          `json:"resolved"`
	Updated  string          `json:"updated"`
}

// asPayload extracts the mutation payload from a Kafka consumer message.
func asPayload(msg *sarama.ConsumerMessage) (*payload, error) {
	payload := &payload{}
	dec := json.NewDecoder(bytes.NewReader(msg.Value))
	dec.UseNumber()
	if err := dec.Decode(payload); err != nil {
		// Empty input is a no-op.
		if errors.Is(err, io.EOF) {
			return payload, nil
		}
		return nil, errors.Wrap(err, "could not decode payload")
	}
	return payload, nil
}
