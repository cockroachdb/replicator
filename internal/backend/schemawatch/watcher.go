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
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
)

// RefreshDelay controls how ofter a Watcher will refresh its schema.
var RefreshDelay = flag.Duration("schemaRefresh", time.Minute,
	"how often to scan for schema changes")

// dbSchema is a simplified representation of a SQL database's schema.
type dbSchema map[ident.Table][]sinktypes.ColData

// A Watcher maintains an internal cache of a database's schema,
// allowing callers to receive notifications of schema changes.
type Watcher struct {
	// All goroutines used by Watch use this as a parent context.
	background context.Context
	dbName     ident.Ident
	delay      time.Duration

	mu struct {
		sync.RWMutex
		cond sync.Cond // Conditional on the RLocker
		data dbSchema
	}

	sql struct {
		tables string
	}
}

var _ sinktypes.Watcher = (*Watcher)(nil)

// newWatcher constructs a new Watcher to monitor the table schema in the
// named database. The returned Watcher will internally refresh
// until the cancel callback is executed.
func newWatcher(
	ctx context.Context, tx pgxtype.Querier, dbName ident.Ident,
) (_ *Watcher, cancel func(), _ error) {
	background, cancel := context.WithCancel(context.Background())

	w := &Watcher{
		background: background,
		delay:      *RefreshDelay,
		dbName:     dbName,
	}
	w.mu.cond.L = w.mu.RLocker()
	w.sql.tables = fmt.Sprintf(tableTemplate, dbName)

	// Initial data load to sanity-check and make ready.
	data, err := w.getTables(ctx, tx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	w.mu.data = data

	go func() {
		for {
			select {
			case <-background.Done():
				return
			case <-time.After(w.delay):
			}

			if err := w.Refresh(background, tx); err != nil {
				log.Printf("could not refresh table data: %v", err)
			}
		}
	}()

	return w, cancel, nil
}

// Refresh immediately refreshes the Watcher's internal cache. This
// is intended for use by tests.
func (w *Watcher) Refresh(ctx context.Context, tx pgxtype.Querier) error {
	data, err := w.getTables(ctx, tx)
	if err != nil {
		log.Printf("could not refresh table data: %v", err)
	}

	w.mu.Lock()
	w.mu.data = data
	w.mu.Unlock()
	w.mu.cond.Broadcast()
	return nil
}

// Snapshot returns the latest known schema for the target database.
func (w *Watcher) Snapshot() map[ident.Table][]sinktypes.ColData {
	w.mu.RLock()
	defer w.mu.RUnlock()

	ret := make(map[ident.Table][]sinktypes.ColData, len(w.mu.data))
	for name, cols := range w.mu.data {
		ret[name] = append(cols[:0], cols...)
	}
	return ret
}

// Watch will send updated column data for the given table until the
// watch is canceled. The requested table must already be known to the
// Watcher.
func (w *Watcher) Watch(table ident.Table) (_ <-chan []sinktypes.ColData, cancel func(), _ error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if _, ok := w.mu.data[table]; !ok {
		return nil, nil, errors.Errorf("unknown table %s", table)
	}

	ctx, cancel := context.WithCancel(w.background)
	ch := make(chan []sinktypes.ColData, 1)

	go func() {
		defer close(ch)

		// All code below is read-locked, so we can't miss updates.
		w.mu.cond.L.Lock()
		defer w.mu.cond.L.Unlock()

		var last []sinktypes.ColData
		for {
			next, ok := w.mu.data[table]
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
					log.Fatal("ColData watcher excessively behind")
				}
			}

			w.mu.cond.Wait()
		}
	}()
	return ch, cancel, nil
}

const tableTemplate = `SELECT schema_name, table_name FROM [SHOW TABLES FROM %s]`

func (w *Watcher) getTables(
	ctx context.Context, tx pgxtype.Querier,
) (dbSchema, error) {
	var ret dbSchema
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.Query(ctx, w.sql.tables)
		if err != nil {
			return err
		}
		defer rows.Close()

		ret = make(dbSchema)
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
			ret[tbl] = cols
		}
		return nil
	})

	return ret, errors.Wrap(err, w.sql.tables)
}
