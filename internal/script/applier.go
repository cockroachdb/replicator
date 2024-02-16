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
	"github.com/cockroachdb/cdc-sink/internal/util/pjson"
	"github.com/dop251/goja"
	"github.com/pkg/errors"
)

// A JS function which is provided with an array of applyOp.
type applyJS func(opData []*applyOp) *goja.Promise

type applyOp struct {
	Action string         `goja:"action"` // Always populated. Type must be string.
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

// applier implements [types.TableAcceptor] to allow user-defined functions to
// be used to interact with the database, rather than using cdc-sink's
// built-in SQL.
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

	muts := batch.Data
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
		return err
	}

	if err := pjson.Decode(ctx, data, func(i int) []byte {
		if data := muts[i].Data; len(data) > 0 {
			return data
		}
		return []byte("null")
	}); err != nil {
		return err
	}

	tx := &targetTX{
		ctx:     ctx,
		applier: a,
		tq:      opts.TargetQuerier,
	}

	var promise *goja.Promise
	if err := a.parent.execTrackedJS(tx, func(rt *goja.Runtime) error {
		promise = a.apply(ops)
		return nil
	}); err != nil {
		return err
	}

	_, err := a.parent.await(ctx, promise)
	return err
}
