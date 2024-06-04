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

package cdcjson

import (
	"encoding/json"
	"io"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// MutationReader is the function that extracts a mutation from a from a
// Reader whose contents are a single line of the input.
type MutationReader func(io.Reader) (types.Mutation, error)

// BulkMutationReader returns a function that reads a mutation from a
// regular changefeed.
func BulkMutationReader() MutationReader {
	return func(reader io.Reader) (types.Mutation, error) {
		var payload struct {
			After   json.RawMessage `json:"after"`
			Before  json.RawMessage `json:"before"`
			Key     json.RawMessage `json:"key"`
			Updated string          `json:"updated"`
		}
		if err := Decode(reader, &payload); err != nil {
			return types.Mutation{}, err
		}
		if payload.Updated == "" {
			return types.Mutation{},
				errors.New("CREATE CHANGEFEED must specify the 'WITH updated' option")
		}

		// Parse the timestamp into nanos and logical.
		ts, err := hlc.Parse(payload.Updated)
		if err != nil {
			return types.Mutation{}, err
		}
		return types.Mutation{
			Before: payload.Before,
			Data:   payload.After,
			Time:   ts,
			Key:    payload.Key,
		}, nil
	}
}

// QueryMutationReader reads a mutation from a query changefeed.
func QueryMutationReader(keys *ident.Map[int]) MutationReader {
	return func(reader io.Reader) (types.Mutation, error) {
		payload := queryPayload{
			keys: keys,
		}
		if err := Decode(reader, &payload); err != nil {
			return types.Mutation{}, err
		}
		return payload.AsMutation()
	}
}
