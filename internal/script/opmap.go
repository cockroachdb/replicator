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
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/dop251/goja"
	"github.com/pkg/errors"
)

// A opMap is a function that allows transformations
// between different kinds of operations on mutations.
//
//	{ Iterable<ApplyOp>) }  => { Promise<Iterable<ApplyOp>> }
type opMapJS func(opData []*applyOp) *goja.Promise

// mapper allows user to process incoming mutation and transform them as needed before
// they are applied to the target.
type mapper struct {
	*tableEnv
	op opMapJS
}

var _ types.ApplyMapper = (*mapper)(nil)

// newMapper creates a new mapper
func newMapper(parent *UserScript, table ident.Table, op opMapJS) *mapper {
	return &mapper{
		op: op,
		tableEnv: &tableEnv{
			parent: parent,
			table:  table,
		},
	}
}

// Map implements [types.ApplyMapper].
func (m *mapper) Map(
	ctx context.Context, tq types.TargetQuerier, muts []types.Mutation,
) ([]types.Mutation, error) {
	ops, err := applyOpsFromMuts(ctx, muts)
	if err != nil {
		return nil, err
	}
	tx := &targetTX{
		ctx:      ctx,
		tq:       tq,
		tableEnv: m.tableEnv,
	}

	var promise *goja.Promise
	if err := m.parent.execTrackedJS(tx, func(rt *goja.Runtime) error {
		promise = m.op(ops)
		return nil
	}); err != nil {
		return nil, err
	}

	res, err := m.parent.await(ctx, promise)
	if err != nil {
		return nil, err
	}
	switch exported := res.Export().(type) {
	case []*applyOp:
		return mutsFromApplyOps(ctx, exported)
	case []any:
		// If we create a new array within the javascript env
		// the result type is []any.
		// We need to check every element.
		ops := make([]*applyOp, len(exported))
		for idx, r := range exported {
			if o, ok := r.(*applyOp); ok {
				ops[idx] = o
			} else {
				return nil, errors.Errorf("unexpected type %T", r)
			}
		}
		return mutsFromApplyOps(ctx, ops)
	default:
		return nil, errors.Errorf("unexpected type %T", exported)
	}
}
