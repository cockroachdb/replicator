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
	"reflect"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
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
	data   *notify.Var[*types.SchemaData]
	delay  time.Duration
	schema ident.Schema
}

var _ types.Watcher = (*watcher)(nil)

// newWatcher constructs a new watcher to monitor the table schema in the
// named database. The returned watcher will internally refresh
// until the cancel callback is executed.
func newWatcher(
	ctx *stopper.Context, tx *types.TargetPool, schema ident.Schema, b Backup,
) (*watcher, error) {
	w := &watcher{
		delay:  *RefreshDelay,
		schema: schema,
	}

	var data *types.SchemaData
	conn, err := tx.Conn(ctx)
	if err != nil || conn.PingContext(ctx) != nil {
		// On start up, when the target database is down, fall back to
		// the staging memo about the table schema
		data, err = b.restore(ctx, schema)
	} else {
		err = conn.Close()
		if err != nil {
			return nil, err
		}

		// Initial data load to sanity-check and make ready.
		data, err = w.getTables(ctx, tx)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	w.data = notify.VarOf(data)

	b.startUpdates(ctx, w.data, schema)

	if w.delay > 0 {
		ctx.Go(func(ctx *stopper.Context) error {
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
	// Ignore notification channel.
	ret, _ := w.data.Get()
	return ret
}

// Refresh immediately refreshes the watcher's internal cache. This
// is intended for use by tests.
func (w *watcher) Refresh(ctx context.Context, tx *types.TargetPool) error {
	next, err := w.getTables(ctx, tx)
	if err != nil {
		return err
	}

	prev, _ := w.data.Get()
	if !reflect.DeepEqual(prev, next) {
		w.data.Set(next)
	}
	return nil
}

// Snapshot returns the known tables in the given user-defined schema.
func (w *watcher) Snapshot(in ident.Schema) *types.SchemaData {
	data := w.Get()

	ret := &types.SchemaData{
		Columns: &ident.TableMap[[]types.ColData]{},
		Order:   make([][]ident.Table, 0, len(data.Order)),
	}

	_ = data.Columns.Range(func(table ident.Table, cols []types.ColData) error {
		if in.Contains(table) {
			// https://github.com/golang/go/wiki/SliceTricks#copy
			out := make([]types.ColData, len(cols))
			copy(out, cols)
			ret.Columns.Put(table, out)
		}
		return nil
	})
	for _, tables := range data.Order {
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
func (w *watcher) Watch(
	ctx *stopper.Context, table ident.Table,
) (*notify.Var[[]types.ColData], error) {
	snapshot := w.Get()
	lastCols, ok := snapshot.Columns.Get(table)
	if !ok {
		return nil, errors.Errorf("unknown table %s", table)
	}
	into := notify.VarOf(lastCols)

	ctx.Go(func(ctx *stopper.Context) error {
		_, _ = stopvar.DoWhenChanged(ctx, snapshot, w.data, func(ctx *stopper.Context, _, next *types.SchemaData) error {
			nextCols, ok := next.Columns.Get(table)

			// Respond to dropping the table.
			if !ok {
				log.Tracef("canceling schema watch for %s because it was dropped", table)
				into.Set(nil)
				// Returning any error to break out.
				return context.Canceled
			}

			// Suppress no-op updates.
			if colSliceEqual(lastCols, nextCols) {
				return nil
			}
			lastCols = nextCols

			// Emit a copy of the slice, to ensure the above
			// comparison can't be affected by any pruning.
			cpy := make([]types.ColData, len(nextCols))
			copy(cpy, nextCols)
			into.Set(cpy)
			return nil
		})
		return nil
	})

	return into, nil
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

	err := retry.Retry(ctx, tx, func(ctx context.Context) error {
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
