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
	"encoding/json"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// parseNdjsonQueryMutation is a parseMutation function.
// When using CDC queries the SELECT Statement must include the event (as "__event__")
// returned by the event_op() function.
// SELECT *, event_op() as operation
// See (https://www.cockroachlabs.com/docs/stable/cdc-queries.html#cdc-query-function-support)
func (h *Handler) parseNdjsonQueryMutation(req *request, rawBytes []byte) (types.Mutation, error) {
	keys, err := h.getPrimaryKey(req)
	if err != nil {
		return types.Mutation{}, err
	}
	qp := queryPayload{
		keys: keys,
	}
	if err := json.Unmarshal(rawBytes, &qp); err != nil {
		return types.Mutation{}, err
	}
	return qp.AsMutation()
}

// getPrimaryKey returns a map that contains all the columns that make up the primary key
// for the target table and their ordinal position within the key.
func (h *Handler) getPrimaryKey(req *request) (*ident.Map[int], error) {
	if req.keys != nil {
		return req.keys, nil
	}
	table, ok := req.target.(ident.Table)
	if !ok {
		return nil, errors.Errorf("expecting ident.Table, got %T", req.target)
	}
	tgt, err := h.Targets.getTarget(table.Schema())
	if err != nil {
		return nil, err
	}
	columns, ok := tgt.watcher.Get().Columns.Get(table)
	if !ok {
		return nil, errors.Errorf("table %q not found", table)
	}
	req.keys = &ident.Map[int]{}
	for i, col := range columns {
		if col.Primary {
			req.keys.Put(col.Name, i)
		}
	}
	return req.keys, nil
}
