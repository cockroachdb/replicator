// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package base provides enough functionality to connect to a database,
// but does not provide any other services. This package is primary used
// to break dependency cycles in tests.
package base

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// TestSet is used by wire.
var TestSet = wire.NewSet(
	ProvideContext,
	ProvideDBInfo,
	ProvideStagingDB,
	ProvideStagingPool,
	ProvideTargetPool,
	ProvideTestDB,

	wire.Struct(new(Fixture), "*"),
)

// Fixture can be used for tests that "just need a database",
// without the other services provided by the target package. One can be
// constructed by calling NewFixture.
type Fixture struct {
	Context     context.Context   // The context for the test.
	DBInfo      *DBInfo           // TODO(bob): Extract version elsewhere?
	StagingPool types.StagingPool // Access to __cdc_sink database.
	TargetPool  types.TargetPool  // Access to the destination. Same as StagingPool.
	StagingDB   ident.StagingDB   // The _cdc_sink SQL DATABASE.
	TestDB      TestDB            // A unique SQL DATABASE identifier.
}

// A global counter for allocating all temp tables in a test run. We
// know that the enclosing database has a unique name, but it's
// convenient for all test table names to be unique as well.
var tempTable int32

// CreateTable creates a test table within the TestDB. The
// schemaSpec parameter must have exactly one %s substitution parameter
// for the database name and table name.
func (f *Fixture) CreateTable(ctx context.Context, schemaSpec string) (TableInfo, error) {
	tableNum := atomic.AddInt32(&tempTable, 1)
	tableName := ident.New(fmt.Sprintf("_test_table_%d", tableNum))
	table := ident.NewTable(f.TestDB.Ident(), ident.Public, tableName)

	err := retry.Execute(ctx, f.TargetPool, fmt.Sprintf(schemaSpec, table))
	return TableInfo{f.DBInfo, table}, errors.WithStack(err)
}

var caseTimout = flag.Duration(
	"caseTimout",
	2*time.Minute,
	"raise this value when debugging to allow individual tests to run longer",
)

// key is a typesafe context key used by Context().
type key struct{}

// ProvideContext returns an execution context that is associated with a
// singleton connection to a CockroachDB cluster.
func ProvideContext() (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), *caseTimout)
	dbInfo, err := bootstrap(ctx)
	if err != nil {
		return nil, cancel, err
	}
	ctx = context.WithValue(ctx, key{}, dbInfo)
	return ctx, cancel, nil
}

// ProvideDBInfo extracts the info from the given context.
func ProvideDBInfo(ctx context.Context) (*DBInfo, error) {
	info, ok := ctx.Value(key{}).(*DBInfo)
	if !ok {
		return nil, errors.New("enclosing context not created by ProvideContext")
	}
	return info, nil
}

// ProvideStagingDB create a globally-unique SQL database. The cancel
// function will drop the database.
func ProvideStagingDB(
	ctx context.Context, pool types.StagingPool,
) (ident.StagingDB, func(), error) {
	ret, cancel, err := CreateDatabase(ctx, pool, "_cdc_sink")
	return ident.StagingDB(ret), cancel, err
}

// ProvideStagingPool exports the database connection.
func ProvideStagingPool(db *DBInfo) types.StagingPool {
	return types.StagingPool{Pool: db.Pool()}
}

// ProvideTargetPool exports the database connection.
func ProvideTargetPool(db *DBInfo) types.TargetPool {
	return types.TargetPool{Pool: db.Pool()}
}

// ProvideTestDB create a globally-unique SQL database. The cancel
// function will drop the database.
func ProvideTestDB(ctx context.Context, pool types.TargetPool) (TestDB, func(), error) {
	ret, cancel, err := CreateDatabase(ctx, pool, "_test_db")
	return TestDB(ret), cancel, err
}

// Ensure unique database identifiers within a test run.
var dbIdentCounter int32

// CreateDatabase creates a SQL DATABASE with a unique name that will be
// dropped when the cancel function is called.
func CreateDatabase(
	ctx context.Context, pool types.Querier, prefix string,
) (ident.Ident, func(), error) {
	dbNum := atomic.AddInt32(&dbIdentCounter, 1)

	// Each package tests run in a separate binary, so we need a
	// "globally" unique ID.  While PIDs do recycle, they're highly
	// unlikely to do so during a single run of the test suite.
	name := ident.New(fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), dbNum))

	cancel := func() {
		err := retry.Execute(ctx, pool, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", name))
		log.WithError(err).WithField("target", name).Debug("dropped database")
	}

	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		"CREATE DATABASE %s", name)); err != nil {
		return ident.Ident{}, cancel, errors.WithStack(err)
	}

	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		`ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds = 600`, name)); err != nil {
		return ident.Ident{}, cancel, errors.WithStack(err)
	}

	return name, cancel, nil
}

// TestDB is an injection point that holds the name of a unique SQL DATABASE that holds
// user-provided data.
type TestDB ident.Ident

// Ident returns the underlying database identifier.
func (t TestDB) Ident() ident.Ident { return ident.Ident(t) }
