// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package feed contains types that relate to configuring individual
// feeds.
package feed

import (
	"context"
	_ "embed" // Embed the schema below.
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

//go:embed schema.sql
var schema string

// A Column provides configurability for individual target columns.
type Column struct {
	// Indicates that the Column is present in the enclosing Table.CAS.
	CAS bool
	// A non-zero deadline enables the feature.
	Deadline time.Duration
	// A SQL expression, which refers to the incoming, reified row data.
	// If the Expression is empty, the Target value should be used.
	Expression string
	// The actual column name to update. This name is not necessarily
	// going to be the same as the entry in the Table.Columns field,
	// to support on-the-fly schema migrations.
	Target ident.Ident
	// A synthetic column is injected into the destination table without
	// requiring any equivalent value in the incoming payload. A
	// synthetic column must also have an Expression.
	Synthetic bool
}

// Copy returns a deep copy of the Column.
func (c *Column) Copy() *Column {
	cpy := *c
	return &cpy
}

// Table contains per-destination-table options.
type Table struct {
	// Ordered Columns that should be used for compare-and-set
	// operations within the table. The pointers in this slice will be
	// the same as those retrieved from the Column method.
	CAS []*Column
	// The actual table to update. This name is not necessarily going to
	// be the same as the entry in the Feed's table map to eventually
	// support on-the-fly remapping of data.
	Target ident.Table

	mu struct {
		sync.Mutex
		// A map of source column names to Column instances.
		columns map[ident.Ident]*Column
	}
}

// Column will return a non-nil Column instance to indicate the
// disposition of the named column from the incoming payload.
func (t *Table) Column(feedName ident.Ident) *Column {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.columnLocked(feedName)
}

func (t *Table) columnLocked(feedName ident.Ident) *Column {
	if found, ok := t.mu.columns[feedName]; ok {
		return found
	}

	ret := &Column{
		Target: feedName,
	}
	if t.mu.columns == nil {
		t.mu.columns = make(map[ident.Ident]*Column)
	}
	t.mu.columns[feedName] = ret
	return ret
}

// Copy creates a deep copy of the Table.
func (t *Table) Copy() *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	ret := &Table{
		Target: t.Target,
	}

	// We need to maintain pointer stability for populating the
	// copied CAS slice.
	casMap := make(map[*Column]*Column, len(t.CAS))

	ret.mu.columns = make(map[ident.Ident]*Column, len(t.mu.columns))
	for colID, col := range t.mu.columns {
		cpy := col.Copy()
		ret.mu.columns[colID] = cpy
		if col.CAS {
			casMap[col] = cpy
		}
	}

	// Copy the CAS slice, but use the map above to ensure pointer
	// stability within the resulting struct.
	ret.CAS = make([]*Column, len(t.CAS))
	for idx, col := range t.CAS {
		ret.CAS[idx] = casMap[col]
	}

	return ret
}

// Feed contains per-incoming-feed options.
type Feed struct {
	// If true, incoming mutations should be applied immediately,
	// without waiting for a resolved timestamp.
	Immediate bool
	// The name of the feed, as presented by incoming requests.
	Name ident.Ident
	// The target schema that the Feed is updating. This is used to
	// provide a default value when unmarshalling a YAML configuration.
	Schema ident.Schema
	// Tables maps feed-relative table names to their destination
	// tables. The database ident in the ident.Table value will always
	// be the feed's name; we only care about matching incoming (schema,
	// table) pairs.
	Tables map[ident.Table]*Table
	// The version number of the feed configuration.
	Version int
}

// Copy creates a deep copy of the Feed.
func (f *Feed) Copy() *Feed {
	ret := &Feed{
		Immediate: f.Immediate,
		Name:      f.Name,
		Schema:    f.Schema,
		Version:   f.Version,
	}

	if f.Tables == nil {
		ret.Tables = map[ident.Table]*Table{}
	} else {
		ret.Tables = make(map[ident.Table]*Table, len(f.Tables))
		for k, v := range f.Tables {
			ret.Tables[k] = v.Copy()
		}
	}

	return ret
}

// Feeds manages the configuration of active feeds.
type Feeds struct {
	// Uses mu.Mutex as its Locker.
	activeUpdated *sync.Cond
	db            *pgxpool.Pool

	mu struct {
		sync.Mutex
		active []*Feed
	}

	sql struct {
		feedsTable, tableTable, columnsTable ident.Table
	}
}

// RefreshDelay controls how often a Feeds instance will refresh its
// configuration. If this value is zero or negative, refresh behavior
// will be disabled.
var RefreshDelay = flag.Duration("configRefresh", time.Minute,
	"how often to scan for updated configurations; set to zero to disable")

// New constructs a new Feeds instance.
func New(
	ctx context.Context, db *pgxpool.Pool, stagingDB ident.Ident,
) (_ *Feeds, cancel func(), _ error) {
	cancel = func() {}
	sql := schema
	// Replace instances of _cdc_sink to make testing idempotent.
	if stagingDB != ident.StagingDB {
		sql = strings.ReplaceAll(sql, ident.StagingDB.Raw(), stagingDB.String())
	}
	if _, err := db.Exec(ctx, sql); err != nil {
		return nil, cancel, err
	}

	f := &Feeds{}
	f.activeUpdated = sync.NewCond(&f.mu)
	f.db = db
	f.sql.feedsTable = ident.NewTable(stagingDB, ident.Public, ident.New("feeds"))
	f.sql.tableTable = ident.NewTable(stagingDB, ident.Public, ident.New("feed_tables"))
	f.sql.columnsTable = ident.NewTable(stagingDB, ident.Public, ident.New("feed_columns"))

	// Initial data load.
	if err := f.loadFeeds(ctx); err != nil {
		return nil, cancel, err
	}

	// Refresh loop.
	background, cancel := context.WithCancel(context.Background())
	if d := *RefreshDelay; d > 0 {
		go func(ctx context.Context) {
			hups := make(chan os.Signal, 1)
			signal.Notify(hups, syscall.SIGHUP)
			defer signal.Stop(hups)

			for {
				select {
				case <-hups:
				case <-time.After(d):
				case <-ctx.Done():
					return
				}

				if err := f.loadFeeds(ctx); err != nil {
					log.WithError(err).Warn("unable to refresh configuration")
					continue
				}
			}
		}(background)
	}

	return f, cancel, nil

}

// Watch returns a channel which will emit refreshed Feed configurations.
func (f *Feeds) Watch() (_ <-chan []*Feed, cancel func()) {
	ch := make(chan []*Feed, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(ch)
		defer cancel()

		f.mu.Lock()
		defer f.mu.Unlock()
		for {
			select {
			case ch <- f.mu.active:
			case <-ctx.Done():
				return
			}
			f.activeUpdated.Wait()
		}
	}()

	return ch, cancel
}
