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

// webhookForQuery responds to the v23.1 webhook scheme for cdc feeds with queries.
// We expect the CREATE CHANGE FEED INTO ... AS ... to use the following options:
// envelope="wrapped",format="json",diff
func (h *Handler) webhookForQuery(ctx context.Context, req *request) error {
	table := req.target.(ident.Table)
	target, err := h.Targets.getTarget(table.Schema())
	if err != nil {
		return err
	}

	keys, err := h.getPrimaryKey(req)
	if err != nil {
		return err
	}
	var message struct {
		Payload []json.RawMessage `json:"payload"`
		Length  int               `json:"length"`
		// With envelope="bare" (default for queries), there is a  `__crdb__` property.
		Bare json.RawMessage `json:"__crdb__"`
		// With envelope="wrapped" there is `resolved` property.
		Resolved string `json:"resolved"`
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
	// Bare messages are not longer supported.
	if message.Bare != nil {
		return errors.New(bareEnvelopeErrorMsg)
	}
	// Check if it is a resolved message.
	if message.Resolved != "" {
		timestamp, err := hlc.Parse(message.Resolved)
		if err != nil {
			return err
		}
		req.timestamp = timestamp
		return h.resolved(ctx, req)
	}
	// Aggregate the mutations by target table. We know that the default
	// batch size for webhooks is reasonable.
	toProcess := &types.MultiBatch{}
	for _, payload := range message.Payload {
		qp := queryPayload{
			keys: keys,
		}
		if err := qp.UnmarshalJSON(payload); err != nil {
			return err
		}
		mut, err := qp.AsMutation()
		if err != nil {
			return err
		}
		// Discard phantom deletes.
		if mut.IsDelete() && mut.Key == nil {
			continue
		}
		if err := toProcess.Accumulate(table, mut); err != nil {
			return err
		}
	}
	return target.acceptor.AcceptMultiBatch(ctx, toProcess, &types.AcceptOptions{})
}
