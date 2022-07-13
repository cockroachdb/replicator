// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sinktest

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/target/resolve"
	"github.com/cockroachdb/cdc-sink/internal/target/tblconf"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// BaseFixture can be used for tests that "just need a database",
// without the other services provided by the target package. One can be
// constructed by calling NewBaseFixture.
type BaseFixture struct {
	Context   context.Context // The context for the test.
	DBInfo    *DBInfo         // TODO(bob): Extract version elsewhere?
	Pool      *pgxpool.Pool   // Access to the database.
	StagingDB ident.StagingDB // The _cdc_sink SQL DATABASE.
	TestDB    TestDB          // A unique SQL DATABASE identifier.
}

// A global counter for allocating all temp tables in a test run. We
// know that the enclosing database has a unique name, but it's
// convenient for all test table names to be unique as well.
var tempTable int32

// CreateTable creates a test table within the TestDB. The
// schemaSpec parameter must have exactly one %s substitution parameter
// for the database name and table name.
func (f *BaseFixture) CreateTable(ctx context.Context, schemaSpec string) (TableInfo, error) {
	tableNum := atomic.AddInt32(&tempTable, 1)
	tableName := ident.New(fmt.Sprintf("_test_table_%d", tableNum))
	table := ident.NewTable(f.TestDB.Ident(), ident.Public, tableName)

	err := retry.Execute(ctx, f.Pool, fmt.Sprintf(schemaSpec, table))
	return TableInfo{f.DBInfo, table}, errors.WithStack(err)
}

// Fixture provides a complete set of database-backed services. One can
// be constructed by calling NewFixture or by incorporating TestSet into
// a Wire provider set.
type Fixture struct {
	BaseFixture

	Appliers   types.Appliers
	Configs    *tblconf.Configs
	Fans       *fan.Fans
	Resolvers  types.Resolvers
	Stagers    types.Stagers
	TimeKeeper types.TimeKeeper
	Watchers   types.Watchers

	MetaTable resolve.MetaTable // The _cdc_sink.resolved_timestamps table.
	Watcher   types.Watcher     // A watcher for TestDB.
}

// CreateTable creates a test table within the TestDB and refreshes the
// target database's Watcher. The schemaSpec parameter must have exactly
// one %s substitution parameter for the database name and table name.
func (f *Fixture) CreateTable(ctx context.Context, schemaSpec string) (TableInfo, error) {
	ti, err := f.BaseFixture.CreateTable(ctx, schemaSpec)
	if err == nil {
		err = f.Watcher.Refresh(ctx, f.Pool)
	}
	return ti, err
}
