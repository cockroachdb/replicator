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
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
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
	background context.Context
	delay      time.Duration
	schema     ident.Schema

	mu struct {
		sync.RWMutex
		data    *types.SchemaData
		updated chan struct{} // Closed and replaced when data is updated.
	}

	sql struct {
		tables string
	}
}

var _ types.Watcher = (*watcher)(nil)

// newWatcher constructs a new watcher to monitor the table schema in the
// named database. The returned watcher will internally refresh
// until the cancel callback is executed.
func newWatcher(
	ctx context.Context, tx *types.TargetPool, schema ident.Schema,
) (_ *watcher, cancel func(), _ error) {
	background, cancel := context.WithCancel(context.Background())

	w := &watcher{
		background: background,
		delay:      *RefreshDelay,
		schema:     schema,
	}
	w.mu.updated = make(chan struct{})
	w.sql.tables = fmt.Sprintf(tableTemplate, schema)

	// Initial data load to sanity-check and make ready.
	data, err := w.getTables(ctx, tx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	w.mu.data = data

	if w.delay > 0 {
		go func() {
			for {
				select {
				case <-background.Done():
					return
				case <-time.After(w.delay):
				}
				if err := w.Refresh(background, tx); err != nil {
					log.WithError(err).WithField("target", schema).Warn("schema refresh failed")
				}
			}
		}()
	}

	return w, cancel, nil
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
		Columns: make(map[ident.Table][]types.ColData, len(w.mu.data.Columns)),
		Order:   make([][]ident.Table, 0, len(w.mu.data.Order)),
	}

	for table, cols := range w.mu.data.Columns {
		if in.Contains(table) {
			// https://github.com/golang/go/wiki/SliceTricks#copy
			out := make([]types.ColData, len(cols))
			copy(out, cols)
			ret.Columns[table] = out
		}
	}
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
func (w *watcher) Watch(table ident.Table) (_ <-chan []types.ColData, cancel func(), _ error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if _, ok := w.mu.data.Columns[table]; !ok {
		return nil, nil, errors.Errorf("unknown table %s", table)
	}

	ctx, cancel := context.WithCancel(w.background)
	ch := make(chan []types.ColData, 1)

	go func() {
		defer close(ch)

		var last []types.ColData
		for {
			w.mu.RLock()
			next, ok := w.mu.data.Columns[table]
			updated := w.mu.updated
			w.mu.RUnlock()

			// Respond to context cancellation or dropping the table.
			if !ok || ctx.Err() != nil {
				return
			}

			// We're read-locked, so this isn't hugely critical.
			if !colSliceEqual(last, next) {
				select {
				case <-ctx.Done():
					return
				case ch <- next:
					last = next
				default:
					log.WithField("target", table).Warn("ColData watcher excessively behind")
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-updated:
				continue
			}
		}
	}()
	return ch, cancel, nil
}

const tableTemplate = `SELECT table_name FROM [SHOW TABLES FROM %s] WHERE type = 'table'`

func (w *watcher) getTables(ctx context.Context, tx *types.TargetPool) (*types.SchemaData, error) {
	ret := &types.SchemaData{
		Columns: make(map[ident.Table][]types.ColData),
	}
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.QueryContext(ctx, w.sql.tables)
		if err != nil {
			return errors.Wrap(err, w.sql.tables)
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				return err
			}
			tbl := ident.NewTable(w.schema, ident.New(table))
			cols, err := getColumns(ctx, tx, tbl)
			if err != nil {
				return err
			}
			ret.Columns[tbl] = cols

		}

		ret.Order, err = getDependencyOrder(ctx, tx, w.schema)
		return err
	})

	return ret, err
}
