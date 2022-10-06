// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/jackc/pgtype/pgxtype"
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
	dbName     ident.Ident
	delay      time.Duration

	cond sync.Cond // Condition on mu's RLocker.
	mu   struct {
		sync.RWMutex
		data *types.SchemaData
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
	ctx context.Context, tx pgxtype.Querier, dbName ident.Ident,
) (_ *watcher, cancel func(), _ error) {
	background, cancel := context.WithCancel(context.Background())

	w := &watcher{
		background: background,
		delay:      *RefreshDelay,
		dbName:     dbName,
	}
	w.cond.L = w.mu.RLocker()
	w.sql.tables = fmt.Sprintf(tableTemplate, dbName)

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
					log.WithError(err).WithField("target", dbName).Warn("schema refresh failed")
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
func (w *watcher) Refresh(ctx context.Context, tx pgxtype.Querier) error {
	data, err := w.getTables(ctx, tx)
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.mu.data = data
	w.mu.Unlock()
	w.cond.Broadcast()
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

		// All code below is read-locked, so we can't miss updates.
		w.cond.L.Lock()
		defer w.cond.L.Unlock()

		var last []types.ColData
		for {
			next, ok := w.mu.data.Columns[table]
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

			w.cond.Wait()
		}
	}()
	return ch, cancel, nil
}

const tableTemplate = `SELECT schema_name, table_name FROM [SHOW TABLES FROM %s]`

func (w *watcher) getTables(ctx context.Context, tx pgxtype.Querier) (*types.SchemaData, error) {
	ret := &types.SchemaData{
		Columns: make(map[ident.Table][]types.ColData),
	}
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.Query(ctx, w.sql.tables)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var schema, table string
			if err := rows.Scan(&schema, &table); err != nil {
				return err
			}
			tbl := ident.NewTable(w.dbName, ident.New(schema), ident.New(table))
			cols, err := getColumns(ctx, tx, tbl)
			if err != nil {
				return err
			}
			ret.Columns[tbl] = cols

		}

		ret.Order, err = getDependencyOrder(ctx, tx, w.dbName)
		return err
	})

	return ret, errors.Wrap(err, w.sql.tables)
}
