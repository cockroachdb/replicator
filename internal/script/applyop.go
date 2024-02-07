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
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/pjson"
)

type applyOp struct {
	Action string         `goja:"action"` // Always populated. Type must be string.
	Data   map[string]any `goja:"data"`   // Present in upsert mode.
	Meta   map[string]any `goja:"meta"`   // Present in upsert mode. Equivalent to map() meta.
	PK     []any          `goja:"pk"`     // Always populated.
}

// applyOpsFromMuts converts a slice of types.Mutation into a
// slice of *applyOp, suitable to be used in  a userscript.
func applyOpsFromMuts(ctx context.Context, muts []types.Mutation) ([]*applyOp, error) {
	ops := make([]*applyOp, len(muts))
	pks := make([]*[]any, len(muts))
	data := make([]*map[string]any, len(muts))
	for idx, mut := range muts {
		mode := actionUpsert
		if mut.IsDelete() {
			mode = actionDelete
		}
		ops[idx] = &applyOp{
			Action: string(mode),
			Meta:   mut.Meta,
		}
		pks[idx] = &ops[idx].PK
		data[idx] = &ops[idx].Data
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
	return ops, nil
}

type applyAction string

const (
	actionDelete applyAction = "delete"
	actionUpsert applyAction = "upsert"
)
