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
	"sort"
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

// dbSchema is a simplified representation of a SQL database's schema.
type tableInfo struct {
	columns []types.ColData
	level   int
}
type dbSchema map[ident.Table]tableInfo
type tablesByLevel []ident.Table

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
		data   dbSchema
		levels tablesByLevel
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
	w.sql.tables = fmt.Sprintf(tableTemplate, dbName, dbName, dbName)

	// Initial data load to sanity-check and make ready.
	data, levels, err := w.getTables(ctx, tx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	w.mu.data = data
	w.mu.levels = levels
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

// Refresh immediately refreshes the watcher's internal cache. This
// is intended for use by tests.
func (w *watcher) Refresh(ctx context.Context, tx pgxtype.Querier) error {
	data, levels, err := w.getTables(ctx, tx)
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.mu.data = data
	w.mu.levels = levels
	w.mu.Unlock()
	w.cond.Broadcast()
	return nil
}

// Snapshot returns the known tables in the database.
func (w *watcher) Snapshot() types.DatabaseTables {
	w.mu.RLock()
	defer w.mu.RUnlock()

	ret := make(map[ident.Table]types.TableSchema, len(w.mu.data))
	for table, cols := range w.mu.data {
		// https://github.com/golang/go/wiki/SliceTricks#copy
		out := make([]types.ColData, len(cols.columns))
		copy(out, cols.columns)
		ret[table] = types.TableSchema{
			Columns: out,
		}
	}
	lout := make([]ident.Table, len(w.mu.levels))
	copy(lout, w.mu.levels)
	return types.DatabaseTables{
		Tables:           ret,
		TablesSortedByFK: lout,
	}
}

// Watch will send updated column data for the given table until the
// watch is canceled. The requested table must already be known to the
// watcher.
func (w *watcher) Watch(table ident.Table) (_ <-chan []types.ColData, cancel func(), _ error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if _, ok := w.mu.data[table]; !ok {
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
			tableData, ok := w.mu.data[table]
			// Respond to context cancellation or dropping the table.
			if !ok || ctx.Err() != nil {
				return
			}
			next := tableData.columns
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

// Get all the tables and 'level' of dependencies based on FK constraints
// To prevent infinite loops caused by cyclic dependencies set an upper limit on recursive calls.
// Level 0 tables don't have FK constraints.
// Level N tables reference at least one table at Level N-1,
//                 and possibly other tables at Level N-1 or lower levels.
const tableTemplate = `
WITH RECURSIVE
	limits AS (SELECT count(*) + 1 AS maxdepth FROM [SHOW TABLES FROM %s]),
	refs
		AS (
			SELECT
				0 AS depth, '' AS referenced_table_name, schema_name, table_name
			FROM
				[SHOW TABLES FROM %s]
			UNION ALL
				SELECT
					refs.depth + 1 AS depth,
					s.referenced_table_name,
					schema_name,
					s.table_name
				FROM
					refs, %s.information_schema.referential_constraints AS s
				WHERE
					refs.table_name = s.referenced_table_name
					AND refs.depth < (SELECT maxdepth FROM limits)
		)
SELECT
	schema_name, table_name, max(depth) AS depth
FROM
	refs
GROUP BY
	schema_name, table_name
ORDER BY
	depth;
`

func (w *watcher) getTables(
	ctx context.Context, tx pgxtype.Querier,
) (dbSchema, tablesByLevel, error) {
	var ret dbSchema
	var levels map[int][]ident.Table
	maxDepth := 0
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.Query(ctx, w.sql.tables)
		if err != nil {
			return err
		}

		defer rows.Close()
		ret = make(dbSchema)
		levels = make(map[int][]ident.Table)

		for rows.Next() {
			var schema, table string
			var depth int
			if err := rows.Scan(&schema, &table, &depth); err != nil {
				return err
			}
			tbl := ident.NewTable(w.dbName, ident.New(schema), ident.New(table))
			log.Tracef("Found table %s level %d \n", tbl, depth)
			cols, err := getColumns(ctx, tx, tbl)
			if err != nil {
				return err
			}
			t := tableInfo{
				columns: cols,
				level:   depth,
			}
			ret[tbl] = t
			levels[depth] = append(levels[depth], tbl)
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return nil
	})

	if err != nil {
		return nil, nil, errors.Wrap(err, w.sql.tables)
	}

	sortedTables := make([]ident.Table, 0, len(ret))
	for l := 0; l <= maxDepth; l++ {
		if l > len(ret) {
			return nil, nil, errors.New("detected cycle in FK references")
		}
		tables := levels[l]
		sort.SliceStable(tables, func(i, j int) bool {
			return tables[i].String() < tables[j].String()
		})
		sortedTables = append(sortedTables, tables...)
	}

	return ret, sortedTables, nil
}
