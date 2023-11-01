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

// Package dlq writes unhandled mutations to dead-letter queues in the
// target database.
package dlq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

const dlqTableMissing = `the dead-letter queue table %[1]s must be created in the target database.
Consider using the following schema:

`

type dlq struct {
	name string
	stmt *sql.Stmt
}

var _ types.DLQ = (*dlq)(nil)

// Enqueue implements [types.DLQ].
func (d *dlq) Enqueue(ctx context.Context, tx types.TargetQuerier, mut types.Mutation) error {
	stmt := d.stmt
	// Bind the prepared statement to the current transaction.
	if sqlTx, ok := tx.(*sql.Tx); ok {
		stmt = sqlTx.Stmt(stmt)
	}
	// We're using JSON-type columns. To avoid ambiguity, we prefer
	// them to be NOT NULL and contain a literal null token if there's
	// no data there.
	after := string(mut.Data)
	if len(after) == 0 {
		after = "null"
	}
	before := string(mut.Before)
	if len(before) == 0 {
		before = "null"
	}
	_, err := stmt.ExecContext(ctx, d.name, mut.Time.Nanos(), mut.Time.Logical(), after, before)
	return errors.WithStack(err)
}

type dlqs struct {
	cfg        *Config
	targetPool *types.TargetPool
	watchers   types.Watchers

	mu struct {
		sync.RWMutex
		validated ident.TableMap[*dlq]
	}
}

var _ types.DLQs = (*dlqs)(nil)

// Get implements [types.DLQs]. It will perform a one-time validation
// that the DLQ table has been defined in the target schema.
func (d *dlqs) Get(ctx context.Context, target ident.Schema, name string) (types.DLQ, error) {
	tbl := ident.NewTable(target, d.cfg.TableName)

	d.mu.RLock()
	found, ok := d.mu.validated.Get(tbl)
	d.mu.RUnlock()
	if ok {
		return found, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check idiom.
	if found, ok := d.mu.validated.Get(tbl); ok {
		return found, nil
	}

	watcher, err := d.watchers.Get(target)
	if err != nil {
		return nil, err
	}
	cols, ok := watcher.Get().Columns.Get(tbl)
	if !ok {
		msg := dlqTableMissing + BasicSchemas[d.targetPool.Product]
		return nil, errors.Errorf(msg, tbl)
	}

	knownCols := ident.Map[struct{}]{}
	for _, col := range cols {
		knownCols.Put(col.Name, struct{}{})
	}

	var missing strings.Builder
	for _, name := range expectedColumns {
		if _, found := knownCols.Get(name); !found {
			if missing.Len() > 0 {
				missing.WriteString(", ")
			}
			missing.WriteString(name.Raw())
		}
	}
	if missing.Len() > 0 {
		return nil, errors.Errorf("dlq table %s was found, but it is missing the following columns: %s",
			tbl, missing.String())
	}

	// The query differs only in the argument syntax.
	var q string
	switch d.targetPool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		q = qBase + argsPG
	case types.ProductOracle:
		q = qBase + argsOra
	case types.ProductMariaDB, types.ProductMySQL:
		q = qBase + argsMySQL
	default:
		return nil, errors.Errorf("dlq unimplemented for product %s", d.targetPool.Product)
	}

	// Attach a prepared statement to the pool. It will be bound to a
	// future transaction as necessary.
	stmt, err := d.targetPool.PrepareContext(ctx, fmt.Sprintf(q, tbl))
	if err != nil {
		return nil, errors.Wrapf(err, "could not prepare DLQ statement: %s", q)
	}

	ret := &dlq{
		name: name,
		stmt: stmt,
	}
	d.mu.validated.Put(tbl, ret)
	return ret, nil
}
