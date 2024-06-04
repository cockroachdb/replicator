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

// Package ndjson provides utilities to decode ndjson changefeed.
package ndjson

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// Parser provides the functionality to process changefeed events encoded
// as ndjson.
type Parser struct {
	BufferSize int
}

// Parse reads a stream of mutations. Each mutation is extracted by
// calling the provided MutationParser function.
func (p *Parser) Parse(
	table ident.Table, parseMutation MutationParser, reader io.Reader,
) (*types.MultiBatch, error) {
	batch := &types.MultiBatch{}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, p.BufferSize), p.BufferSize)
	for scanner.Scan() {
		buf := scanner.Bytes()
		if len(buf) == 0 {
			continue
		}
		mut, err := parseMutation(bytes.NewBuffer(buf))
		if err != nil {
			return nil, err
		}
		// Discard phantom deletes.
		if mut.IsDelete() && mut.Key == nil {
			continue
		}
		if err := batch.Accumulate(table, mut); err != nil {
			return nil, err
		}
	}
	return batch, scanner.Err()
}

// Resolved extracts the resolved timestamp
func (p *Parser) Resolved(reader io.Reader) (hlc.Time, error) {
	var payload struct {
		Resolved string `json:"resolved"`
	}
	if err := Decode(reader, &payload); err != nil {
		return hlc.Zero(), err
	}
	if payload.Resolved == "" {
		return hlc.Zero(),
			errors.New("CREATE CHANGEFEED must specify the 'WITH resolved' option")
	}
	// Parse the timestamp into nanos and logical.
	return hlc.Parse(payload.Resolved)
}

// Decode a JSON object.
func Decode(r io.Reader, v any) error {
	// Large numbers are not turned into strings, so the UseNumber option for
	// the decoder is required.
	dec := json.NewDecoder(r)
	dec.UseNumber()
	return dec.Decode(v)
}

// MutationParser is the function that extracts a mutation from a stream.
type MutationParser func(io.Reader) (types.Mutation, error)

// ParseMutation extracts a mutation.
func ParseMutation(reader io.Reader) (types.Mutation, error) {
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
