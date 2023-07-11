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
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// webhookForQuery responds to the v23.1 webhook scheme for cdc feeds with queries.
// we expect the select statement in the CREATE CHANGE FEED to use the following convention:
// the SELECT Statement must have a column "__event_op__" returned by the event_op() function.
// SELECT *, event_op() as __event_op__
func (h *Handler) webhookForQuery(ctx context.Context, req *request) error {
	table := req.target.(ident.Table)
	keys, err := h.getPrimaryKey(ctx, req)
	if err != nil {
		return err
	}
	var message struct {
		Payload  []json.RawMessage `json:"payload"`
		Length   int               `json:"length"`
		Metadata Metadata          `json:"__crdb__"`
	}

	dec := json.NewDecoder(req.body)
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&message); err != nil {
		// Empty input is a no-op.
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errors.Wrap(err, "could not decode payload")
	}
	if message.Metadata.Resolved != "" {
		timestamp, err := hlc.Parse(message.Metadata.Resolved)
		if err != nil {
			return err
		}
		req.timestamp = timestamp
		return h.resolved(ctx, req)
	}
	// Aggregate the mutations by target table. We know that the default
	// batch size for webhooks is reasonable.
	toProcess := make(map[ident.Table][]types.Mutation)
	for _, payload := range message.Payload {
		qp := queryPayload{
			keys: keys,
		}
		dec := json.NewDecoder(bytes.NewReader(payload))
		if err := dec.Decode(&qp); err != nil {
			return err
		}
		mut, err := qp.AsMutation()
		if err != nil {
			return err
		}
		toProcess[table] = append(toProcess[table], mut)
	}
	return h.processMutations(ctx, toProcess)
}
