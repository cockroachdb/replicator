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

import (
	"context"
	"encoding/json"
	"io"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// webhook responds to the v21.2 webhook scheme.
// https://www.cockroachlabs.com/docs/stable/create-changefeed.html#responses
func (h *Handler) webhook(ctx context.Context, req *request) error {
	var payload struct {
		Payload []struct {
			After   json.RawMessage `json:"after"`
			Before  json.RawMessage `json:"before"`
			Key     json.RawMessage `json:"key"`
			Topic   string          `json:"topic"`
			Updated string          `json:"updated"`
		} `json:"payload"`
		Length   int    `json:"length"`
		Resolved string `json:"resolved"`
	}
	dec := json.NewDecoder(req.body)
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		// Empty input is a no-op.
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errors.Wrap(err, "could not decode payload")
	}
	conveyor, err := h.Conveyors.Get(req.target.Schema())
	if err != nil {
		return err
	}

	if payload.Resolved != "" {
		timestamp, err := hlc.Parse(payload.Resolved)
		if err != nil {
			return err
		}
		req.timestamp = timestamp

		return h.resolved(ctx, req)
	}

	// Aggregate the mutations by target table. We know that the default
	// batch size for webhooks is reasonable.
	toProcess := &types.MultiBatch{}

	for i := range payload.Payload {
		timestamp, err := hlc.Parse(payload.Payload[i].Updated)
		if err != nil {
			return err
		}

		table, qual, err := ident.ParseTableRelative(payload.Payload[i].Topic, req.target.Schema())
		if err != nil {
			return err
		}
		// Ensure the destination table is in the target schema.
		if qual != ident.TableOnly {
			table = ident.NewTable(req.target.Schema(), table.Table())
		}

		mut := types.Mutation{
			Before: payload.Payload[i].Before,
			Data:   payload.Payload[i].After,
			Key:    payload.Payload[i].Key,
			Time:   timestamp,
		}
		if err := toProcess.Accumulate(table, mut); err != nil {
			return err
		}
	}

	return conveyor.AcceptMultiBatch(ctx, toProcess, &types.AcceptOptions{})
}
