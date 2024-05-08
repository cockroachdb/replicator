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
	"strings"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/pjson"
	"github.com/dop251/goja"
	"github.com/pkg/errors"
)

// A JS function which is provided with an array of applyOp.
type applyJS func(opData []*applyOp) *goja.Promise

type applyOp struct {
	Action string         `goja:"action"` // Always populated. Type must be string.
	Before map[string]any `goja:"before"` // Present if diff is enabled.
	Data   map[string]any `goja:"data"`   // Present in upsert mode.
	Meta   map[string]any `goja:"meta"`   // Present in upsert mode. Equivalent to map() meta.
	PK     []any          `goja:"pk"`     // Always populated.
}

type applyAction string

const (
	actionDelete applyAction = "delete"
	actionUpsert applyAction = "upsert"
)

// notInTransaction is used to provide a helpful error message if
// api.getTX() is called when no transaction is available.
func notInTransaction() error {
	return errors.New("no transaction is currently open")
}

// applier implements [types.TableAcceptor] to allow user-defined
// functions to be used to interact with the database, rather than using
// Replicator's built-in SQL.
type applier struct {
	apply  applyJS
	parent *UserScript
	table  ident.Table
}

var _ types.TableAcceptor = (*applier)(nil)

func newApplier(parent *UserScript, table ident.Table, apply applyJS) *applier {
	return &applier{
		apply:  apply,
		parent: parent,
		table:  table,
	}
}

// AcceptTableBatch implements [types.TableAcceptor].
func (a *applier) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if opts == nil || opts.TargetQuerier == nil {
		return errors.New("UserScript apply function cannot be called without an explicit transaction")
	}
	ops, err := batchToOps(ctx, batch)
	if err != nil {
		return err
	}

	tx := &targetTX{
		ctx:           ctx,
		applier:       a,
		batchTemplate: batch.Empty(),
		tq:            opts.TargetQuerier,
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

// batchToOps constructs a new slice of applyOp to represent the
// operations in the batch.
func batchToOps(ctx context.Context, batch *types.TableBatch) ([]*applyOp, error) {
	muts := batch.Data
	before := make([]*map[string]any, len(muts))
	data := make([]*map[string]any, len(muts))
	ops := make([]*applyOp, len(muts))
	pks := make([]*[]any, len(muts))
	for idx, mut := range muts {
		mode := actionUpsert
		if mut.IsDelete() {
			mode = actionDelete
		}
		ops[idx] = &applyOp{
			Action: string(mode),
			Meta:   mut.Meta,
		}
		before[idx] = &ops[idx].Before
		data[idx] = &ops[idx].Data
		pks[idx] = &ops[idx].PK
	}

	if err := pjson.Decode(ctx, pks, func(i int) []byte {
		return muts[i].Key
	}); err != nil {
		return nil, err
	}

	if err := pjson.Decode(ctx, data, func(i int) []byte {
		if raw := muts[i].Data; len(raw) > 0 {
			return raw
		}
		return []byte("null")
	}); err != nil {
		return nil, err
	}

	if err := pjson.Decode(ctx, before, func(i int) []byte {
		if raw := muts[i].Before; len(raw) > 0 {
			return raw
		}
		return []byte("null")
	}); err != nil {
		return nil, err
	}

	return ops, nil
}

// opsIntoBatch replaces the [types.TableBatch.Data] field with a new
// slice of mutations based on the applyOp. The other fields of the
// batch are expected to have been initialized e.g. via
// [types.TableBatch.Empty].
func opsIntoBatch(ctx context.Context, ops []*applyOp, batch *types.TableBatch) error {
	pks := make([][]byte, len(ops))
	if err := pjson.Encode(ctx, pks, func(i int) []any {
		return ops[i].PK
	}); err != nil {
		return err
	}

	data := make([][]byte, len(ops))
	if err := pjson.Encode(ctx, data, func(i int) map[string]any {
		return ops[i].Data
	}); err != nil {
		return err
	}

	before := make([][]byte, len(ops))
	if err := pjson.Encode(ctx, before, func(i int) map[string]any {
		return ops[i].Before
	}); err != nil {
		return err
	}

	batch.Data = make([]types.Mutation, len(ops))
	for idx, op := range ops {
		batch.Data[idx] = types.Mutation{
			Before: before[idx],
			Data:   data[idx],
			Key:    pks[idx],
			Meta:   op.Meta,
			Time:   batch.Time,
		}
		switch applyAction(strings.ToLower(op.Action)) {
		case actionDelete:
			// Absence of data means delete.
			batch.Data[idx].Data = nil
		case actionUpsert:
			// OK.
		default:
			return errors.Errorf("unknown ApplyOp action %q", op.Action)
		}
	}
	return nil
}
