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

// Package applycfg contains code for persisting applier configurations.
// This is separate from apply since we use the staging database as the
// storage location.
package applycfg

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/cmap"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// configZero is a sentinel value for "no configuration".
var configZero = NewConfig()

// Configs provides a lookup service for per-destination-table
// configurations.
type Configs struct {
	pool *types.StagingPool

	// Parent context of all watch behaviors. When the background
	// refresh loop is stopped, we can cancel all watches as there will
	// be nothing to cause them to fire.
	watchCtx context.Context

	mu struct {
		sync.RWMutex
		data    *ident.TableMap[*Config]
		updated chan struct{} // Closed and swapped when data is updated.
	}

	sql struct {
		delete  string
		loadAll string
		upsert  string
	}
}

// Diagnostic implements [diag.Diagnostic].
func (c *Configs) Diagnostic(_ context.Context) any {
	return c.GetAll()
}

// Get returns the configuration for the named table, or a non-nil, zero
// value if no configuration has been provided.
func (c *Configs) Get(tbl ident.Table) *Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ret, ok := c.mu.data.Get(tbl); ok {
		return ret
	}
	return configZero
}

// GetAll returns a deep copy of all known table configurations.
func (c *Configs) GetAll() *ident.TableMap[*Config] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data := c.mu.data

	ret := &ident.TableMap[*Config]{}
	data.CopyInto(ret)
	return ret
}

const (
	confSchema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_schema STRING CHECK ( length(target_schema) > 0 ),
  target_table  STRING CHECK ( length(target_table) > 0 ),
  target_column STRING CHECK ( length(target_column) > 0 ),

  cas_order INT        NOT NULL DEFAULT 0 CHECK ( cas_order >= 0 ),
  deadline  INTERVAL   NOT NULL DEFAULT 0::INTERVAL,
  expr      STRING     NOT NULL DEFAULT '',
  extras    BOOL       NOT NULL DEFAULT false,
  ignore    BOOL       NOT NULL DEFAULT false,
  src_name  STRING     NOT NULL DEFAULT '',

  PRIMARY KEY (target_schema, target_table, target_column)
)
`
	deleteConfTemplate = `
DELETE FROM %[1]s WHERE target_schema = $1 AND target_table = $2`
	loadConfTemplate = `
SELECT target_schema, target_table, target_column,
       cas_order, deadline, expr, extras, ignore, src_name
FROM %[1]s`
	upsertConfTemplate = `
UPSERT INTO %[1]s (target_schema, target_table, target_column,
  cas_order, deadline, expr, extras, ignore, src_name)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9
)`
)

// Refresh triggers an immediate reload of the table configurations.
// This method is intended for use by tests. Under normal circumstances,
// the configuration is automatically refreshed.
func (c *Configs) Refresh(ctx context.Context) (changed bool, _ error) {
	rows, err := c.pool.Query(ctx, c.sql.loadAll)
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer rows.Close()

	// Accumulate CAS data in a sparse map and then validate it.
	type tempConfig struct {
		*Config
		casMap map[int]SourceColumn
	}
	nextConfigs := &ident.TableMap[*tempConfig]{}

	for rows.Next() {
		var targetSchema, targetTable, targetColumn string
		var cas int // 1-based index; 0 == regular column
		var deadline time.Duration
		var expr string
		var extras bool
		var ignore bool
		var rename string

		err := rows.Scan(
			&targetSchema, &targetTable, &targetColumn,
			&cas, &deadline, &expr, &extras, &ignore, &rename)
		if err != nil {
			return false, errors.WithStack(err)
		}

		sch, err := ident.ParseSchema(targetSchema)
		if err != nil {
			return false, err
		}

		targetTableIdent := ident.NewTable(sch, ident.New(targetTable))
		targetColIdent := ident.New(targetColumn)

		tableData, found := nextConfigs.Get(targetTableIdent)
		if !found {
			tableData = &tempConfig{NewConfig(), make(map[int]SourceColumn)}
			nextConfigs.Put(targetTableIdent, tableData)
		}

		if cas != 0 {
			// Convert to zero-based.
			tableData.casMap[cas-1] = targetColIdent
		}
		if deadline > 0 {
			tableData.Deadlines.Put(targetColIdent, deadline)
		}
		if expr != "" {
			tableData.Exprs.Put(targetColIdent, expr)
		}
		if extras {
			if !tableData.Extras.Empty() {
				return false, errors.Errorf(
					"column %s already configured as extras column",
					tableData.Extras)
			}
			tableData.Extras = targetColIdent
		}
		if ignore {
			tableData.Ignore.Put(targetColIdent, true)
		}
		if rename != "" {
			tableData.SourceNames.Put(targetColIdent, ident.New(rename))
		}

	}

	finalized := &ident.TableMap[*Config]{}
	if err := nextConfigs.Range(func(table ident.Table, data *tempConfig) error {
		// Ensure that the CAS mappings are sane and create the slice.
		data.CASColumns = make(SourceColumns, len(data.casMap))
		for idx := range data.CASColumns {
			colName, found := data.casMap[idx]
			if !found {
				return errors.Errorf("%s: gap in CAS columns at index %d", table, idx)
			}
			data.CASColumns[idx] = colName
		}
		finalized.Put(table, data.Config)
		return nil
	}); err != nil {
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cmap.Equal[ident.Table, *Config](c.mu.data, finalized, func(a, b *Config) bool {
		return a.Equal(b)
	}) {
		return false, nil
	}

	c.mu.data = finalized
	// Wake all watches.
	close(c.mu.updated)
	c.mu.updated = make(chan struct{})
	return true, nil
}

// refreshLoop calls the Refresh method periodically or in response
// to a SIGHUP.
func (c *Configs) refreshLoop(ctx context.Context) {
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
		case <-hupCh:
		}
		if changed, err := c.Refresh(ctx); err != nil {
			log.WithError(err).Warn("could not refresh table configuration")
		} else if changed {
			log.Trace("refreshed table configuration")
		}
	}
}

// Store will replace the stored table configuration with the one
// provided. An empty or nil Config will delete any existing
// configuration. This method does not refresh the internal map,
// instead, call Refresh once the associated database transaction has
// committed.
func (c *Configs) Store(
	ctx context.Context, tx types.StagingQuerier, table ident.Table, cfg *Config,
) error {
	table = table.Canonical()
	// Delete existing configuration data for the table.
	if _, err := tx.Exec(ctx,
		c.sql.delete,
		table.Schema().Raw(),
		table.Table().Raw(),
	); err != nil {
		return errors.WithStack(err)
	}

	// Nothing else to do.
	if cfg == nil || cfg.IsZero() {
		return nil
	}

	// Collect all referenced source columns.
	var casIdx ident.Map[int]
	var refs ident.Map[struct{}]
	for idx, col := range cfg.CASColumns {
		refs.Put(col, struct{}{})
		casIdx.Put(col, idx+1) // Store one-based values.
	}
	_ = cfg.Deadlines.Range(func(col ident.Ident, val time.Duration) error {
		if val > 0 {
			refs.Put(col, struct{}{})
		}
		return nil
	})
	_ = cfg.Exprs.Range(func(col ident.Ident, val string) error {
		if val != "" {
			refs.Put(col, struct{}{})
		}
		return nil
	})
	if !cfg.Extras.Empty() {
		refs.Put(cfg.Extras, struct{}{})
	}
	_ = cfg.Ignore.Range(func(col ident.Ident, val bool) error {
		if val {
			refs.Put(col, struct{}{})
		}
		return nil
	})
	_ = cfg.SourceNames.Range(func(col ident.Ident, val SourceColumn) error {
		if !val.Empty() {
			refs.Put(col, struct{}{})
		}
		return nil
	})

	// Insert relevant data for each referenced column. We rely on the
	// zero values returned from map lookup misses.
	return refs.Range(func(col ident.Ident, _ struct{}) error {
		_, err := tx.Exec(ctx,
			c.sql.upsert,
			table.Schema().Raw(),
			table.Table().Raw(),
			col.Raw(),
			casIdx.GetZero(col),
			cfg.Deadlines.GetZero(col),
			cfg.Exprs.GetZero(col),
			ident.Equal(cfg.Extras, col),
			cfg.Ignore.GetZero(col),
			cfg.SourceNames.GetZero(col).Raw(),
		)
		return errors.WithStack(err)
	})
}

// Watch returns a channel that will emit updated Config information.
// The cancel function should be called when the consumer is no longer
// interested in updates.
func (c *Configs) Watch(tbl ident.Table) (ch <-chan *Config, cancel func()) {
	tbl = tbl.Canonical()
	ret := make(chan *Config)

	// See discussion in c.watchCtx for why this isn't Background().
	ctx, cancel := context.WithCancel(c.watchCtx)
	go func() {
		defer close(ret)
		cfg := c.waitForUpdate(ctx, tbl, nil)
		for cfg != nil {
			select {
			case <-ctx.Done():
				return
			case ret <- cfg:
				cfg = c.waitForUpdate(ctx, tbl, cfg)
			}
		}
	}()

	return ret, cancel
}

// waitForUpdate blocks until the configuration for the given table has
// changed from the old value. This method returns the updated
// configuration for the table, or nil on context cancellation.
func (c *Configs) waitForUpdate(ctx context.Context, tbl ident.Table, last *Config) *Config {
	// Wait for the config pointer to have changed. This will indicate
	// that a refresh took place. We also check for context
	// cancellation, since the refresh loop will send one final
	// broadcast when the parent watch context has been shut down.
	for {
		c.mu.RLock()
		next, ok := c.mu.data.Get(tbl)
		if !ok {
			next = configZero
		}
		waitFor := c.mu.updated
		c.mu.RUnlock()

		// Deep compare the per-table data, since the channel is
		// replaced whenever any table configuration is refreshed.
		if next != last && !last.Equal(next) {
			return next
		}
		last = next

		select {
		case <-ctx.Done():
			return nil
		case <-waitFor:
			continue
		}
	}
}
