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
	"database/sql"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/dop251/goja"
	"github.com/pkg/errors"
)

// targetTX is a facade passed to the userscript to expose the target
// database transaction and various other metadata.
type targetTX struct {
	*applier

	ctx     context.Context     // Passed to database methods.
	columns []map[string]any    // Lazily-constructed schema data.
	tq      types.TargetQuerier // The database transaction.
	mu      sync.Mutex          // Serializes access to methods on tq.
}

var _ asyncTracker = (*targetTX)(nil)

// Columns is exported to the userscript. It will lazily populate the
// columns field.
func (tx *targetTX) Columns() []map[string]any {
	if len(tx.columns) > 0 {
		return tx.columns
	}
	cols := tx.parent.watcher.Get().Columns.GetZero(tx.table)
	for _, col := range cols {
		// Keep in sync with .d.ts file.
		m := map[string]any{
			"ignored": col.Ignored,
			"name":    col.Name.String(),
			"primary": col.Primary,
			"type":    col.Type,
		}
		// It's JS-idiomatic for the string to be null than empty.
		if col.DefaultExpr != "" {
			m["defaultExpr"] = col.DefaultExpr
		}
		tx.columns = append(tx.columns, m)
	}
	return tx.columns
}

// Enter implements [asyncTracker]. It will inject the targetTX into the
// runtime so the user code may use it.
func (tx *targetTX) enter(script *UserScript) error {
	return script.apiModule.Set("getTX", func() *targetTX {
		return tx
	})
}

// Exec is exported to the userscript.
func (tx *targetTX) Exec(q string, args ...any) *goja.Promise {
	// Only called from JS, so we know that rtMu is locked.
	promise, resolve, reject := tx.parent.rt.NewPromise()

	// Execute the SQL in a (pooled) background goroutine.
	tx.parent.execTask(func() {
		tx.mu.Lock()
		_, err := tx.tq.ExecContext(tx.ctx, q, args...)
		tx.mu.Unlock()
		err = errors.Wrap(err, q)

		// Ignoring error since closure never returns an error.
		_ = tx.parent.execJS(func(rt *goja.Runtime) error {
			if err == nil {
				resolve(goja.Undefined())
			} else {
				reject(err)
			}
			return nil
		})
	})

	return promise
}

// Exit implements [asyncTracker]. It will clean up the references set
// by [targetTX.enter].
func (tx *targetTX) exit(script *UserScript) error {
	return script.apiModule.Set("getTX", notInTransaction)
}

// Query is exported to the userscript.
func (tx *targetTX) Query(q string, args ...any) *goja.Promise {
	// Only called from JS, so we know that rtMu is locked.
	promise, resolve, reject := tx.parent.rt.NewPromise()

	// Execute the SQL in a (pooled) background goroutine.
	tx.parent.execTask(func() {
		tx.mu.Lock()
		rows, err := tx.tq.QueryContext(tx.ctx, q, args...)
		tx.mu.Unlock()

		// Extract the number of columns for the result iterator.
		var numCols int
		if err == nil {
			var names []string
			names, err = rows.Columns()
			numCols = len(names)
		}
		err = errors.Wrap(err, q)

		// Once the results are ready, re-enter the JS runtime mutex to
		// fulfil the promise. Ignoring error since closure never
		// returns an error.
		_ = tx.parent.execJS(func(rt *goja.Runtime) error {
			if err != nil {
				reject(err)
				return nil
			}

			// Construct the iterator JS object by setting
			// Symbol.iterator to a function that returns a value which
			// implements the iterator protocol (i.e. has a next()
			// function).
			obj := rt.NewObject()
			if err := obj.SetSymbol(goja.SymIterator, func() *rowsIter {
				return &rowsIter{numCols, rows}
			}); err != nil {
				return errors.WithStack(err)
			}
			resolve(obj)
			return nil
		})
	})

	return promise
}

// Schema is exported to the userscript.
func (tx *targetTX) Schema() string {
	return tx.table.Schema().String()
}

// Table is exported to the userscript.
func (tx *targetTX) Table() string {
	return tx.table.String()
}

// rowsIter exports a [sql.Rows] into a JS API that conforms to the
// iterator protocol. Note that goja does not (as of this writing)
// support the async iterable protocol.
//
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols
// https://pkg.go.dev/github.com/dop251/goja#example-Object.SetSymbol
type rowsIter struct {
	colCount int
	rows     *sql.Rows
}

// Next implements the JS iterator protocol.
func (it *rowsIter) Next() (*rowsIterResult, error) {
	next := it.rows.Next()
	err := it.rows.Err()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !next {
		return &rowsIterResult{Done: true}, nil
	}

	data := make([]any, it.colCount)
	ptrs := make([]any, len(data))
	for idx := range ptrs {
		ptrs[idx] = &data[idx]
	}
	if err := it.rows.Scan(ptrs...); err != nil {
		return nil, errors.WithStack(err)
	}
	return &rowsIterResult{Value: data}, nil
}

// Return implements the JS iterator protocol and will be called
// if the iterator is not being read to completion. This allows us
// to preemptively close the rowset.
func (it *rowsIter) Return() *rowsIterResult {
	_ = it.rows.Close()
	return &rowsIterResult{Done: true}
}

// Implements the JS iteration result protocol.
type rowsIterResult struct {
	Done  bool  `goja:"done"`
	Value []any `goja:"value"`
}
