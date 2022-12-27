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
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/wire"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// TestSet contains providers to create a self-contained Fixture.
var TestSet = wire.NewSet(
	target.Set,

	ProvideContext,
	ProvideDBInfo,
	ProvidePool,
	ProvideTestDB,
	ProvideWatcher,
	ProvideStagingDB,

	wire.Bind(new(pgxtype.Querier), new(*pgxpool.Pool)),
	wire.Struct(new(BaseFixture), "*"),
	wire.Struct(new(Fixture), "*"),
)

// TestDB is an injection point that holds the name of a unique SQL DATABASE that holds
// user-provided data.
type TestDB ident.Ident

// Ident returns the underlying database identifier.
func (t TestDB) Ident() ident.Ident { return ident.Ident(t) }

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

// ProvideWatcher is called by Wire to construct a Watcher
// bound to the testing database.
func ProvideWatcher(
	ctx context.Context, testDB TestDB, watchers types.Watchers,
) (types.Watcher, error) {
	return watchers.Get(ctx, testDB.Ident())
}

// ProvidePool is a convenience provider for the pgx pool type.
func ProvidePool(db *DBInfo) *pgxpool.Pool { return db.Pool() }

// ProvideStagingDB create a globally-unique SQL database. The cancel
// function will drop the database.
func ProvideStagingDB(ctx context.Context, pool *pgxpool.Pool) (ident.StagingDB, func(), error) {
	ret, cancel, err := provideTestDB(ctx, pool, "_cdc_sink")
	return ident.StagingDB(ret), cancel, err
}

// ProvideTestDB create a globally-unique SQL database. The cancel
// function will drop the database.
func ProvideTestDB(ctx context.Context, pool *pgxpool.Pool) (TestDB, func(), error) {
	ret, cancel, err := provideTestDB(ctx, pool, "_test_db")
	return TestDB(ret), cancel, err
}

// Ensure unique database identifiers within a test run.
var dbIdentCounter int32

// provideTestDB creates a SQL DATABASE with a unique name that will be
// dropped when the cancel function is called.
func provideTestDB(
	ctx context.Context, pool pgxtype.Querier, prefix string,
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
