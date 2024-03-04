// Copyright 2023 The Cockroach Authors
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

package cdc

// This file contains code repackaged from url.go.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// parseMutation takes a single line from an ndjson and extracts enough
// information to be able to persist it to the staging table.
type parseMutation func(*request, []byte) (types.Mutation, error)

// parseNdjsonMutation is a parseMutation function
func parseNdjsonMutation(_ *request, rawBytes []byte) (types.Mutation, error) {
	var payload struct {
		After   json.RawMessage `json:"after"`
		Before  json.RawMessage `json:"before"`
		Key     json.RawMessage `json:"key"`
		Updated string          `json:"updated"`
	}
	// Large numbers are not turned into strings, so the UseNumber option for
	// the decoder is required.
	dec := json.NewDecoder(bytes.NewReader(rawBytes))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
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

// ndjson parses an incoming block of ndjson files and stores the
// associated Mutations. This assumes that the underlying
// Stager will store duplicate values in an idempotent manner,
// should the request fail partway through.
func (h *Handler) ndjson(ctx context.Context, req *request, parser parseMutation) error {
	table := req.target.(ident.Table)
	batch := &types.MultiBatch{}

	scanner := bufio.NewScanner(req.body)
	// Our config defaults to bufio.MaxScanTokenSize.
	scanner.Buffer(make([]byte, 0, h.Config.NDJsonBuffer), h.Config.NDJsonBuffer)
	for scanner.Scan() {
		buf := scanner.Bytes()
		if len(buf) == 0 {
			continue
		}
		mut, err := parser(req, buf)
		if err != nil {
			return err
		}
		// Discard phantom deletes.
		if mut.IsDelete() && mut.Key == nil {
			continue
		}
		if err := batch.Accumulate(table, mut); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	target, err := h.Targets.getTarget(table.Schema())
	if err != nil {
		return err
	}

	return target.acceptor.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{})
}
