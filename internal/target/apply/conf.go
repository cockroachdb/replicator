// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

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
		len(t.Ignore) == 0 &&
		len(t.SourceNames) == 0
}

// Configs provides a lookup service for per-destination-table
// configurations.
type Configs struct {
	dataChanged *sync.Cond
	pool        pgxtype.Querier

	// Parent context of all watch behaviors. When the background
	// refresh loop is stopped, we can cancel all watches as there will
	// be nothing to cause them to fire.
	watchCtx context.Context

	mu struct {
		sync.Mutex
		data map[ident.Table]*Config
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret, ok := c.mu.data[tbl]
	if !ok {
		ret = configZero
	}
	return ret
}

// GetAll returns a deep copy of all known table configurations.
func (c *Configs) GetAll() map[ident.Table]*Config {
	c.mu.Lock()
	data := c.mu.data
	c.mu.Unlock()

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
  source_column STRING CHECK ( length(source_column) > 0 ),

  cas_order INT        NOT NULL DEFAULT 0 CHECK ( cas_order >= 0 ),
  deadline  INTERVAL   NOT NULL DEFAULT 0::INTERVAL,
  expr      STRING     NOT NULL DEFAULT '',
  ignore    BOOL       NOT NULL DEFAULT false,
  src_name  STRING     NOT NULL DEFAULT '',

  PRIMARY KEY (target_db, target_schema, target_table, source_column)
)
`
	deleteConfTemplate = `
DELETE FROM %[1]s WHERE target_db = $1 AND target_schema = $2 AND target_table = $3`
	loadConfTemplate = `
SELECT target_db, target_schema, target_table, source_column,
       cas_order, deadline, expr, ignore, src_name
FROM %[1]s`
	upsertConfTemplate = `
UPSERT INTO %[1]s (target_db, target_schema, target_table, source_column,
  cas_order, deadline, expr, ignore, src_name)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9
)`
)

// Refresh triggers an immediate reload of the table configurations.
// This method is intended for use by tests. Under normal circumstances,
// the configuration is automatically refreshed.
func (c *Configs) Refresh(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, c.sql.loadAll)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()

	// Accumulate CAS data in a sparse map and then validate it.
	type tempConfig struct {
		*Config
		casMap map[int]SourceColumn
	}
	next := make(map[ident.Table]*tempConfig)

	for rows.Next() {
		var targetDB, targetSchema, targetTable, sourceColumn string
		var cas int // 1-based index; 0 == regular column
		var deadline time.Duration
		var expr string
		var ignore bool
		var rename string

		err := rows.Scan(
			&targetDB, &targetSchema, &targetTable, &sourceColumn,
			&cas, &deadline, &expr, &ignore, &rename)
		if err != nil {
			return errors.WithStack(err)
		}

		targetTableIdent := ident.NewTable(
			ident.New(targetDB), ident.New(targetSchema), ident.New(targetTable))
		srcColIdent := ident.New(sourceColumn)

		tableData, found := next[targetTableIdent]
		if !found {
			tableData = &tempConfig{NewConfig(), make(map[int]SourceColumn)}
			next[targetTableIdent] = tableData
		}

		if cas != 0 {
			// Convert to zero-based.
			tableData.casMap[cas-1] = srcColIdent
		}
		if deadline > 0 {
			tableData.Deadlines[srcColIdent] = deadline
		}
		if expr != "" {
			tableData.Exprs[srcColIdent] = expr
		}
		if ignore {
			tableData.Ignore[srcColIdent] = true
		}
		if rename != "" {
			tableData.SourceNames[srcColIdent] = ident.New(rename)
		}

	}

	finalized := make(map[ident.Table]*Config, len(next))
	for table, data := range next {
		// Ensure that the CAS mappings are sane and create the slice.
		data.CASColumns = make([]SourceColumn, len(data.casMap))
		for idx := range data.CASColumns {
			colName, found := data.casMap[idx]
			if !found {
				return errors.Errorf("%s: gap in CAS columns at index %d", table, idx)
			}
			data.CASColumns[idx] = colName
		}
		finalized[table] = data.Config
	}

	c.mu.Lock()
	old := c.mu.data
	c.mu.Unlock()

	if !reflect.DeepEqual(old, finalized) {
		c.mu.Lock()
		c.mu.data = finalized
		c.mu.Unlock()
		// Wake all watches.
		c.dataChanged.Broadcast()
	}

	return nil
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
		if err := c.Refresh(ctx); err != nil {
			log.WithError(err).Warn("could not refresh table configuration")
		} else {
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
	ctx context.Context, tx pgxtype.Querier, table ident.Table, cfg *Config,
) error {
	// Delete existing configuration data for the table.
	_, err := tx.Exec(ctx,
		c.sql.delete,
		table.Database().Raw(),
		table.Schema().Raw(),
		table.Table().Raw())
	if err != nil {
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
		_, err := tx.Exec(ctx,
			c.sql.upsert,
			table.Database().Raw(),
			table.Schema().Raw(),
			table.Table().Raw(),
			col.Raw(),
			casIdx[col],
			cfg.Deadlines[col],
			cfg.Exprs[col],
			cfg.Ignore[col],
			cfg.SourceNames[col].Raw(),
		)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

// Watch returns a channel that will emit updated Config information.
// The cancel function should be called when the consumer is no longer
// interested in updates.
func (c *Configs) Watch(tbl ident.Table) (ch <-chan *Config, cancel func()) {
	ret := make(chan *Config, 1)

	// Load the current value and emit it.
	last := c.Get(tbl)
	ret <- last

	ctx, cancel := context.WithCancel(c.watchCtx)
	go func() {
		defer close(ret)

		for ctx.Err() == nil {
			// Just wait for something interesting to happen.
			c.dataChanged.L.Lock()
			c.dataChanged.Wait()
			c.dataChanged.L.Unlock()

			next := c.Get(tbl)
			// Nothing changed.
			if last == next {
				continue
			}
			// Deep compare since dataChanged fires whenever any table
			// data is refreshed.
			if !reflect.DeepEqual(last, next) {
				select {
				case <-ctx.Done():
					return
				case ret <- next:
				}
			}
			last = next
		}
	}()

	return ret, cancel
}
