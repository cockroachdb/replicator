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

package script

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/pjson"
	"github.com/pkg/errors"
)

type applyOp struct {
	Action string         `goja:"action"` // Always populated. Type must be string.
	Data   map[string]any `goja:"data"`   // Present in upsert mode.
	Meta   map[string]any `goja:"meta"`   // Present in upsert mode. Equivalent to map() meta.
	Before map[string]any `goja:"before"` // Present when before is available in the mutation.
	PK     []any          `goja:"pk"`     // Always populated.
	Time   string         `goja:"time"`   // String representation of hlc.Time
}

// applyOpsFromMuts converts a slice of types.Mutation into a
// slice of *applyOp, suitable to be used in  a userscript.
func applyOpsFromMuts(ctx context.Context, muts []types.Mutation) ([]*applyOp, error) {
	ops := make([]*applyOp, len(muts))
	pks := make([]*[]any, len(muts))
	data := make([]*map[string]any, len(muts))
	before := make([]*map[string]any, len(muts))
	for idx, mut := range muts {
		mode := actionUpsert
		if mut.IsDelete() {
			mode = actionDelete
		}
		ops[idx] = &applyOp{
			Action: string(mode),
			Meta:   mut.Meta,
			Time:   mut.Time.String(),
		}
		pks[idx] = &ops[idx].PK
		data[idx] = &ops[idx].Data
		before[idx] = &ops[idx].Before
	}

	if err := pjson.Decode(ctx, pks, func(i int) []byte {
		return muts[i].Key
	}); err != nil {
		return nil, err
	}

	if err := pjson.Decode(ctx, data, func(i int) []byte {
		if data := muts[i].Data; len(data) > 0 {
			return data
		}
		return []byte("null")
	}); err != nil {
		return nil, err
	}

	if err := pjson.Decode(ctx, before, func(i int) []byte {
		if before := muts[i].Before; len(before) > 0 {
			return before
		}
		return []byte("null")
	}); err != nil {
		return nil, err
	}
	return ops, nil
}

// mutsFromApplyOps converts a slice of *applyOp, returned from
// a userscript into a slice of types.Mutation.
func mutsFromApplyOps(ctx context.Context, ops []*applyOp) ([]types.Mutation, error) {
	muts := make([]types.Mutation, len(ops))
	var err error
	for idx, op := range ops {
		muts[idx].Time, err = hlc.Parse(op.Time)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		muts[idx].Meta = op.Meta
		muts[idx].Key, err = json.Marshal(op.PK)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if bytes.Equal(muts[idx].Key, []byte("null")) {
			muts[idx].Key = nil
		}

		muts[idx].Data, err = json.Marshal(op.Data)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if bytes.Equal(muts[idx].Data, []byte("null")) {
			muts[idx].Data = nil
		}
		muts[idx].Before, err = json.Marshal(op.Before)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if bytes.Equal(muts[idx].Before, []byte("null")) {
			muts[idx].Before = nil
		}
	}
	return muts, nil
}

type applyAction string

const (
	actionDelete applyAction = "delete"
	actionUpsert applyAction = "upsert"
)
