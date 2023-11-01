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

// Package schemawatch contains code to allow the schema of a target
// database to be queried and monitored.
package schemawatch

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// RefreshDelay controls how ofter a watcher will refresh its schema. If
// this value is zero or negative, refresh behavior will be disabled.
var RefreshDelay = flag.Duration("schemaRefresh", time.Minute,
	"how often to scan for schema changes; set to zero to disable")

// A watcher maintains an internal cache of a database's schema,
// allowing callers to receive notifications of schema changes.
type watcher struct {
	// All goroutines used by Watch use this as a parent context.
	background *stopper.Context
	delay      time.Duration
	schema     ident.Schema

	mu struct {
		sync.RWMutex
		data    *types.SchemaData
		updated chan struct{} // Closed and replaced when data is updated.
	}
}

var _ types.Watcher = (*watcher)(nil)

// newWatcher constructs a new watcher to monitor the table schema in the
// named database. The returned watcher will internally refresh
// until the cancel callback is executed.
func newWatcher(ctx *stopper.Context, tx *types.TargetPool, schema ident.Schema) (*watcher, error) {
	w := &watcher{
		background: ctx,
		delay:      *RefreshDelay,
		schema:     schema,
	}
	w.mu.updated = make(chan struct{})

	// Initial data load to sanity-check and make ready.
	data, err := w.getTables(ctx, tx)
	if err != nil {
		return nil, err
	}
	w.mu.data = data

	if w.delay > 0 {
		ctx.Go(func() error {
			for {
				select {
				case <-ctx.Stopping():
					return nil
				case <-time.After(w.delay):
					if err := w.Refresh(ctx, tx); err != nil {
						log.WithError(err).WithField("target", schema).Warn("schema refresh failed")
					}
				}
			}
		})
	}

	return w, nil
}

// Get implements types.Watcher.
func (w *watcher) Get() *types.SchemaData {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.mu.data
}

// Refresh immediately refreshes the watcher's internal cache. This
// is intended for use by tests.
func (w *watcher) Refresh(ctx context.Context, tx *types.TargetPool) error {
	data, err := w.getTables(ctx, tx)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.mu.data = data

	// Close and replace the channel to create a broadcast effect.
	close(w.mu.updated)
	w.mu.updated = make(chan struct{})

	return nil
}

// Snapshot returns the known tables in the given user-defined schema.
func (w *watcher) Snapshot(in ident.Schema) *types.SchemaData {
	w.mu.RLock()
	defer w.mu.RUnlock()

	ret := &types.SchemaData{
		Columns: &ident.TableMap[[]types.ColData]{},
		Order:   make([][]ident.Table, 0, len(w.mu.data.Order)),
	}

	_ = w.mu.data.Columns.Range(func(table ident.Table, cols []types.ColData) error {
		if in.Contains(table) {
			// https://github.com/golang/go/wiki/SliceTricks#copy
			out := make([]types.ColData, len(cols))
			copy(out, cols)
			ret.Columns.Put(table, out)
		}
		return nil
	})
	for _, tables := range w.mu.data.Order {
		filtered := make([]ident.Table, 0, len(tables))
		for _, tbl := range tables {
			if in.Contains(tbl) {
				filtered = append(filtered, tbl)
			}
		}
		if len(filtered) > 0 {
			ret.Order = append(ret.Order, filtered)
		}
	}
	return ret
}

// String is for debugging use only.
func (w *watcher) String() string {
	return fmt.Sprintf("Watcher(%s)", w.schema)
}

// Watch will send updated column data for the given table until the
// watch is canceled. The requested table must already be known to the
// watcher.
func (w *watcher) Watch(table ident.Table) (<-chan []types.ColData, func(), error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if _, ok := w.mu.data.Columns.Get(table); !ok {
		return nil, nil, errors.Errorf("unknown table %s", table)
	}

	ctx := stopper.WithContext(w.background)
	ch := make(chan []types.ColData, 1)

	ctx.Go(func() error {
		defer close(ch)

		var last []types.ColData
		for {
			w.mu.RLock()
			next, ok := w.mu.data.Columns.Get(table)
			updated := w.mu.updated
			w.mu.RUnlock()

			// Respond to context cancellation or dropping the table.
			if !ok || ctx.Err() != nil {
				return nil
			}

			// We're read-locked, so this isn't hugely critical.
			if !colSliceEqual(last, next) {
				select {
				case <-ctx.Stopping():
					return nil
				case ch <- next:
					last = next
				default:
					log.WithField("target", table).Warn("ColData watcher excessively behind")
				}
			}

			select {
			case <-ctx.Stopping():
				return nil
			case <-updated:
				continue
			}
		}
	})
	return ch, func() { ctx.Stop(100 * time.Millisecond) }, nil
}

const (
	databaseTemplateCrdb = `SELECT datname FROM pg_database WHERE datname ILIKE $1`
	tableTemplateCrdb    = `
SELECT table_catalog, table_schema, table_name
  FROM %s.information_schema.tables
 WHERE table_catalog = $1
   AND table_schema ILIKE $2
   AND table_type = 'BASE TABLE'`
	tableTemplateMySQL = `
SELECT table_schema, NULL, table_name
FROM information_schema.tables
WHERE upper(table_schema) = upper(?)
AND table_type = 'BASE TABLE'`
	tableTemplateOracle = `
SELECT OWNER, NULL, TABLE_NAME FROM ALL_TABLES WHERE UPPER(OWNER) = UPPER(:owner)`
)

func (w *watcher) getTables(ctx context.Context, tx *types.TargetPool) (*types.SchemaData, error) {
	ret := &types.SchemaData{
		Columns: &ident.TableMap[[]types.ColData]{},
	}

	err := retry.Retry(ctx, func(ctx context.Context) error {
		var rows *sql.Rows
		var err error

		switch tx.Product {
		case types.ProductCockroachDB, types.ProductPostgreSQL:
			parts := w.schema.Idents(make([]ident.Ident, 0, 2))
			if len(parts) != 2 {
				return errors.Errorf("expecting a schema with 2 parts, got %v", parts)
			}

			// Normalize the database name to however it's defined in
			// the target.
			var dbNameRaw string
			if err := tx.QueryRowContext(ctx, databaseTemplateCrdb, parts[0].Raw()).Scan(&dbNameRaw); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return errors.Errorf("unknown schema: %s", w.schema)
				}
				return errors.Wrap(err, w.schema.String())
			}

			rows, err = tx.QueryContext(ctx,
				fmt.Sprintf(tableTemplateCrdb, ident.New(dbNameRaw)), dbNameRaw, parts[1].Raw())

		case types.ProductMariaDB, types.ProductMySQL:
			rows, err = tx.QueryContext(ctx, tableTemplateMySQL, w.schema.Raw())
		case types.ProductOracle:
			rows, err = tx.QueryContext(ctx, tableTemplateOracle, w.schema.Raw())

		default:
			return errors.Errorf("unimplemented product: %s", tx.Product)
		}

		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()

		var sch ident.Schema
		for rows.Next() {
			// Oracle query may return NULL.
			rawParts := make([]*string, 3)
			if err := rows.Scan(&rawParts[0], &rawParts[1], &rawParts[2]); err != nil {
				return err
			}

			// Filter null or empty values.
			parts := make([]ident.Ident, 0, len(rawParts))
			for _, rawPart := range rawParts {
				if rawPart == nil || *rawPart == "" {
					continue
				}
				parts = append(parts, ident.New(*rawPart))
			}

			// All tables will be in the same enclosing schema, so we
			// only need to compute this once.
			if sch.Empty() {
				sch, err = ident.NewSchema(parts[:len(parts)-1]...)
				if err != nil {
					return err
				}
			}

			tbl := ident.NewTable(sch, parts[len(parts)-1])
			cols, err := getColumns(ctx, tx, tbl)
			if err != nil {
				return err
			}
			ret.Columns.Put(tbl, cols)
		}

		// Empty if there were no tables.
		if !sch.Empty() {
			ret.Order, err = getDependencyOrder(ctx, tx, sch)
		}
		return err
	})

	return ret, err
}
