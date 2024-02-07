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

package script

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/dop251/goja"
)

// A JS function which is provided with an array of applyOp.
type applyJS func(opData []*applyOp) *goja.Promise

// applier implements [types.Applier] to allow user-defined functions to
// be used to interact with the database, rather than using cdc-sink's
// built-in SQL.
type applier struct {
	*tableEnv
	apply applyJS
}

var _ types.Applier = (*applier)(nil)

func newApplier(parent *UserScript, table ident.Table, apply applyJS) *applier {
	return &applier{
		apply: apply,
		tableEnv: &tableEnv{
			parent: parent,
			table:  table,
		},
	}
}

// Apply implements [types.Applier].
func (a *applier) Apply(ctx context.Context, tq types.TargetQuerier, muts []types.Mutation) error {
	ops, err := applyOpsFromMuts(ctx, muts)
	if err != nil {
		return err
	}
	tx := &targetTX{
		ctx:      ctx,
		tableEnv: a.tableEnv,
		tq:       tq,
	}

	var promise *goja.Promise
	if err := a.parent.execTrackedJS(tx, func(rt *goja.Runtime) error {
		promise = a.apply(ops)
		return nil
	}); err != nil {
		return err
	}

	_, err = a.parent.await(ctx, promise)
	return err
}
