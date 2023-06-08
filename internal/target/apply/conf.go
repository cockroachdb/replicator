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

package apply

import (
	"context"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// substitutionToken contains the string that we'll use to substitute in
// the actual parameter index into the generated SQL.
const substitutionToken = "$0"

// Type aliases to improve readability.
type (
	// SourceColumn is the name of a column found in incoming data.
	SourceColumn = ident.Ident
	// TargetColumn is the name of a column found in the target database.
	TargetColumn = ident.Ident
)

// A Config contains per-target-table configuration.
type Config struct {
	CASColumns  []TargetColumn                 // The columns for compare-and-set operations.
	Deadlines   map[TargetColumn]time.Duration // Deadline-based operation.
	Exprs       map[TargetColumn]string        // Synthetic or replacement SQL expressions.
	Extras      TargetColumn                   // JSONB column to store unmapped values in.
	Ignore      map[TargetColumn]bool          // Source column names to ignore.
	SourceNames map[TargetColumn]SourceColumn  // Look for alternate name in the incoming data.
}

// configZero is a sentinel value for "no configuration".
var configZero = NewConfig()

// NewConfig constructs a Config with all map fields populated.
func NewConfig() *Config {
	return &Config{
		Deadlines:   make(types.Deadlines),
		Exprs:       make(map[TargetColumn]string),
		Ignore:      make(map[TargetColumn]bool),
		SourceNames: make(map[TargetColumn]SourceColumn),
	}
}

// Copy returns a copy of the Config.
func (t *Config) Copy() *Config {
	ret := NewConfig()

	ret.CASColumns = append(ret.CASColumns, t.CASColumns...)
	for k, v := range t.Deadlines {
		ret.Deadlines[k] = v
	}
	for k, v := range t.Exprs {
		ret.Exprs[k] = v
	}
	ret.Extras = t.Extras
	for k, v := range t.Ignore {
		ret.Ignore[k] = v
	}
	for k, v := range t.SourceNames {
		ret.SourceNames[k] = v
	}

	return ret
}

// IsZero returns true if the Config represents the absence of a
// configuration.
func (t *Config) IsZero() bool {
	return len(t.CASColumns) == 0 &&
		len(t.Deadlines) == 0 &&
		len(t.Exprs) == 0 &&
		t.Extras.IsEmpty() &&
		len(t.Ignore) == 0 &&
		len(t.SourceNames) == 0
}

// Configs provides a lookup service for per-destination-table
// configurations.
type Configs struct {
	pool types.Querier

	// Parent context of all watch behaviors. When the background
	// refresh loop is stopped, we can cancel all watches as there will
	// be nothing to cause them to fire.
	watchCtx context.Context

	mu struct {
		sync.RWMutex
		data    map[ident.Table]*Config
		updated chan struct{} // Closed and swapped when data is updated.
	}

	sql struct {
		delete  string
		loadAll string
		upsert  string
	}
}

// Get returns the configuration for the named table, or a non-nil, zero
// value if no configuration has been provided.
func (c *Configs) Get(tbl ident.Table) *Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ret, ok := c.mu.data[tbl]; ok {
		return ret
	}
	return configZero
}

// GetAll returns a deep copy of all known table configurations.
func (c *Configs) GetAll() map[ident.Table]*Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data := c.mu.data

	ret := make(map[ident.Table]*Config, len(data))
	for k, v := range data {
		ret[k] = v.Copy()
	}
	return ret
}

const (
	confSchema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_db     STRING CHECK ( length(target_db) > 0 ),
  target_schema STRING CHECK ( length(target_schema) > 0 ),
  target_table  STRING CHECK ( length(target_table) > 0 ),
  target_column STRING CHECK ( length(target_column) > 0 ),

  cas_order INT        NOT NULL DEFAULT 0 CHECK ( cas_order >= 0 ),
  deadline  INTERVAL   NOT NULL DEFAULT 0::INTERVAL,
  expr      STRING     NOT NULL DEFAULT '',
  extras    BOOL       NOT NULL DEFAULT false,
  ignore    BOOL       NOT NULL DEFAULT false,
  src_name  STRING     NOT NULL DEFAULT '',

  PRIMARY KEY (target_db, target_schema, target_table, target_column)
)
`
	deleteConfTemplate = `
DELETE FROM %[1]s WHERE target_db = $1 AND target_schema = $2 AND target_table = $3`
	loadConfTemplate = `
SELECT target_db, target_schema, target_table, target_column,
       cas_order, deadline, expr, extras, ignore, src_name
FROM %[1]s`
	upsertConfTemplate = `
UPSERT INTO %[1]s (target_db, target_schema, target_table, target_column,
  cas_order, deadline, expr, extras, ignore, src_name)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
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
	nextConfigs := make(map[ident.Table]*tempConfig)

	for rows.Next() {
		var targetDB, targetSchema, targetTable, targetColumn string
		var cas int // 1-based index; 0 == regular column
		var deadline time.Duration
		var expr string
		var extras bool
		var ignore bool
		var rename string

		err := rows.Scan(
			&targetDB, &targetSchema, &targetTable, &targetColumn,
			&cas, &deadline, &expr, &extras, &ignore, &rename)
		if err != nil {
			return false, errors.WithStack(err)
		}

		targetTableIdent := ident.NewTable(
			ident.New(targetDB), ident.New(targetSchema), ident.New(targetTable))
		targetColIdent := ident.New(targetColumn)

		tableData, found := nextConfigs[targetTableIdent]
		if !found {
			tableData = &tempConfig{NewConfig(), make(map[int]SourceColumn)}
			nextConfigs[targetTableIdent] = tableData
		}

		if cas != 0 {
			// Convert to zero-based.
			tableData.casMap[cas-1] = targetColIdent
		}
		if deadline > 0 {
			tableData.Deadlines[targetColIdent] = deadline
		}
		if expr != "" {
			tableData.Exprs[targetColIdent] = expr
		}
		if extras {
			if !tableData.Extras.IsEmpty() {
				return false, errors.Errorf(
					"column %s already configured as extras column",
					tableData.Extras)
			}
			tableData.Extras = targetColIdent
		}
		if ignore {
			tableData.Ignore[targetColIdent] = true
		}
		if rename != "" {
			tableData.SourceNames[targetColIdent] = ident.New(rename)
		}

	}

	finalized := make(map[ident.Table]*Config, len(nextConfigs))
	for table, data := range nextConfigs {
		// Ensure that the CAS mappings are sane and create the slice.
		data.CASColumns = make([]SourceColumn, len(data.casMap))
		for idx := range data.CASColumns {
			colName, found := data.casMap[idx]
			if !found {
				return false, errors.Errorf("%s: gap in CAS columns at index %d", table, idx)
			}
			data.CASColumns[idx] = colName
		}
		finalized[table] = data.Config
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if reflect.DeepEqual(c.mu.data, finalized) {
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
	ctx context.Context, tx types.Querier, table ident.Table, cfg *Config,
) error {
	// Delete existing configuration data for the table.
	if _, err := tx.Exec(ctx,
		c.sql.delete,
		table.Database().Raw(),
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
	casIdx := make(map[SourceColumn]int)
	refs := make(map[SourceColumn]struct{})
	for idx, col := range cfg.CASColumns {
		refs[col] = struct{}{}
		casIdx[col] = idx + 1 // Store one-based values.
	}
	for col, val := range cfg.Deadlines {
		if val > 0 {
			refs[col] = struct{}{}
		}
	}
	for col, val := range cfg.Exprs {
		if val != "" {
			refs[col] = struct{}{}
		}
	}
	if !cfg.Extras.IsEmpty() {
		refs[cfg.Extras] = struct{}{}
	}
	for col, val := range cfg.Ignore {
		if val {
			refs[col] = struct{}{}
		}
	}
	for col, val := range cfg.SourceNames {
		if !val.IsEmpty() {
			refs[col] = struct{}{}
		}
	}

	// Insert relevant data for each referenced column. We rely on the
	// zero values returned from map lookup misses.
	for col := range refs {
		if _, err := tx.Exec(ctx,
			c.sql.upsert,
			table.Database().Raw(),
			table.Schema().Raw(),
			table.Table().Raw(),
			col.Raw(),
			casIdx[col],
			cfg.Deadlines[col],
			cfg.Exprs[col],
			cfg.Extras == col,
			cfg.Ignore[col],
			cfg.SourceNames[col].Raw(),
		); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

// Watch returns a channel that will emit updated Config information.
// The cancel function should be called when the consumer is no longer
// interested in updates.
func (c *Configs) Watch(tbl ident.Table) (ch <-chan *Config, cancel func()) {
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
		next, ok := c.mu.data[tbl]
		if !ok {
			next = configZero
		}
		waitFor := c.mu.updated
		c.mu.RUnlock()

		// Deep compare the per-table data, since the channel is
		// replaced whenever any table configuration is refreshed.
		if next != last && !reflect.DeepEqual(last, next) {
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
