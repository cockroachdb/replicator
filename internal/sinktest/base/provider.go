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

// Package base provides enough functionality to connect to a database,
// but does not provide any other services. This package is primary used
// to break dependency cycles in tests.
package base

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
	"github.com/cockroachdb/replicator/internal/util/stmtcache"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	defaultConnString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

	// DummyPassword is baked into the docker-compose configuration.
	DummyPassword = "SoupOrSecret"

	envSourceString  = "TEST_SOURCE_CONNECT"
	envStagingString = "TEST_STAGING_CONNECT"
	envTargetString  = "TEST_TARGET_CONNECT"

	// chosen arbitrarily, we don't really generate this many different
	// SQL statements.
	statementCacheSize = 128
)

var (
	sourceConn   *string
	stagingConn  *string
	targetString *string
)

func init() {
	// We use os.Getenv and a length check so that defined, empty
	// environment variables are ignored.

	sourceConnect := defaultConnString
	if found := os.Getenv(envSourceString); len(found) > 0 {
		sourceConnect = found
	}
	sourceConn = flag.String("testSourceConnect", sourceConnect,
		"the connection string to use for the source db")

	stagingConnect := defaultConnString
	if found := os.Getenv(envStagingString); len(found) > 0 {
		stagingConnect = found
	}
	stagingConn = flag.String("testStagingConnect", stagingConnect,
		"the connection string to use for the staging db")

	targetConnect := defaultConnString
	if found := os.Getenv(envTargetString); len(found) > 0 {
		targetConnect = found
	}
	targetString = flag.String("testTargetConnect", targetConnect,
		"the connection string to use for the target db")
}

// TestSet is used by wire.
var TestSet = wire.NewSet(
	ProvideContext,
	ProvideStagingSchema,
	ProvideStagingPool,
	ProvideSourcePool,
	ProvideSourceSchema,
	ProvideTargetPool,
	ProvideTargetSchema,
	ProvideTargetStatements,
	diag.New,

	wire.Bind(new(context.Context), new(*stopper.Context)),
	wire.Struct(new(Fixture), "*"),
)

// Fixture can be used for tests that "just need a database",
// without the other services provided by the target package. One can be
// constructed by calling NewFixture.
type Fixture struct {
	Context      *stopper.Context        // The context for the test.
	SourcePool   *types.SourcePool       // Access to user-data tables and changefeed creation.
	SourceSchema sinktest.SourceSchema   // A container for tables within SourcePool.
	StagingPool  *types.StagingPool      // Access to _replicator database.
	StagingDB    ident.StagingSchema     // The _replicator SQL DATABASE.
	TargetCache  *types.TargetStatements // Prepared statements.
	TargetPool   *types.TargetPool       // Access to the destination.
	TargetSchema sinktest.TargetSchema   // A container for tables within TargetPool.
}

// CreateSourceTable creates a test table within the SourcePool and
// SourceSchema. The schemaSpec parameter must have exactly one %s
// substitution parameter for the database name and table name.
func (f *Fixture) CreateSourceTable(
	ctx context.Context, schemaSpec string,
) (TableInfo[*types.SourcePool], error) {
	return CreateTable(ctx, f.SourcePool, f.SourceSchema.Schema(), schemaSpec)
}

// CreateTargetTable creates a test table within the TargetPool and
// TargetSchema. The schemaSpec parameter must have exactly one %s
// substitution parameter for the database name and table name.
func (f *Fixture) CreateTargetTable(
	ctx context.Context, schemaSpec string,
) (TableInfo[*types.TargetPool], error) {
	return CreateTable(ctx, f.TargetPool, f.TargetSchema.Schema(), schemaSpec)
}

var caseTimout = flag.Duration(
	"caseTimout",
	2*time.Minute,
	"raise this value when debugging to allow individual tests to run longer",
)

// ProvideContext returns an execution context that is associated with a
// singleton connection to a CockroachDB cluster.
func ProvideContext(t testing.TB) *stopper.Context {
	ctx := stopper.WithContext(context.Background())
	t.Cleanup(func() {
		ctx.Stop(10 * time.Millisecond)
		_ = ctx.Wait()
	})
	ctx.Go(func(ctx *stopper.Context) error {
		select {
		case <-ctx.Stopping():
		// Clean shutdown, do nothing.
		case <-time.After(*caseTimout):
			// Just cancel immediately.
			ctx.Stop(0)
		}
		return nil
	})
	return ctx
}

// ProvideSourcePool connects to the source database. If the source is a
// CockroachDB cluster, this function will also configure the rangefeed
// and license cluster settings if they have not been previously
// configured.
func ProvideSourcePool(ctx *stopper.Context, diags *diag.Diagnostics) (*types.SourcePool, error) {
	tgt := *sourceConn
	log.Infof("source connect string: %s", tgt)
	ret, err := stdpool.OpenTarget(ctx, tgt,
		stdpool.WithDiagnostics(diags, "source"),
		stdpool.WithTestControls(stdpool.TestControls{
			WaitForStartup: true,
		}),
	)
	if err != nil {
		return nil, err
	}

	if ret.Product != types.ProductCockroachDB {
		return (*types.SourcePool)(ret), err
	}

	// Set the cluster settings once, if we need to.
	var enabled bool
	if err := retry.Retry(ctx, ret, func(ctx context.Context) error {
		return ret.QueryRowContext(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled)
	}); err != nil {
		return nil, errors.Wrap(err, "could not check cluster setting")
	}
	if !enabled {
		if lic, ok := os.LookupEnv("COCKROACH_DEV_LICENSE"); ok {
			if err := retry.Execute(ctx, ret,
				"SET CLUSTER SETTING cluster.organization = $1",
				"Cockroach Labs - Production Testing",
			); err != nil {
				return nil, errors.Wrap(err, "could not set cluster.organization")
			}
			if err := retry.Execute(ctx, ret,
				"SET CLUSTER SETTING enterprise.license = $1", lic,
			); err != nil {
				return nil, errors.Wrap(err, "could not set enterprise.license")
			}
		}

		if err := retry.Execute(ctx, ret,
			"SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
			return nil, errors.Wrap(err, "could not enable rangefeeds")
		}
	}

	return (*types.SourcePool)(ret), nil
}

// ProvideStagingSchema create a globally-unique container for tables in the
// staging database.
func ProvideStagingSchema(
	ctx *stopper.Context, pool *types.StagingPool,
) (ident.StagingSchema, error) {
	ret, err := provideSchema(ctx, pool, "cdc")
	return ident.StagingSchema(ret), err
}

// ProvideStagingPool opens a connection to the CockroachDB staging
// cluster under test.
func ProvideStagingPool(ctx *stopper.Context) (*types.StagingPool, error) {
	tgt := *stagingConn
	log.Infof("staging connect string: %s", tgt)
	pool, err := stdpool.OpenPgxAsStaging(ctx, tgt,
		stdpool.WithTestControls(stdpool.TestControls{
			WaitForStartup: true,
		}),
		stdpool.WithConnectionLifetime(time.Minute, 15*time.Second, 5*time.Second),
		stdpool.WithPoolSize(32),
		stdpool.WithTransactionTimeout(2*time.Minute), // Aligns with test case timeout.
	)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

// ProvideTargetPool connects to the target database (which is most
// often the same as the source database).
func ProvideTargetPool(
	ctx *stopper.Context, source *types.SourcePool, diags *diag.Diagnostics,
) (*types.TargetPool, error) {
	tgt := *targetString
	if tgt == source.ConnectionString {
		log.Info("reusing SourcePool as TargetPool")
		return (*types.TargetPool)(source), nil
	}
	log.Infof("target connect string: %s", tgt)
	pool, err := stdpool.OpenTarget(ctx, *targetString,
		stdpool.WithDiagnostics(diags, "target"),
		stdpool.WithTestControls(stdpool.TestControls{
			WaitForStartup: true,
		}),
		stdpool.WithConnectionLifetime(time.Minute, 15*time.Second, 5*time.Second),
		stdpool.WithPoolSize(32),
		stdpool.WithTransactionTimeout(2*time.Minute), // Aligns with test case timeout.
	)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

// ProvideSourceSchema create a globally-unique container for tables in
// the source database.
func ProvideSourceSchema(
	ctx *stopper.Context, pool *types.SourcePool,
) (sinktest.SourceSchema, error) {
	sch, err := provideSchema(ctx, pool, "src")
	log.Infof("source schema: %s", sch)
	return sinktest.SourceSchema(sch), err
}

// ProvideTargetSchema create a globally-unique container for tables in
// the target database.
func ProvideTargetSchema(
	ctx *stopper.Context,
	diags *diag.Diagnostics,
	pool *types.TargetPool,
	stmts *types.TargetStatements,
) (sinktest.TargetSchema, error) {
	sch, err := provideSchema(ctx, pool, "tgt")
	ret := sinktest.TargetSchema(sch)
	if err != nil {
		return ret, err
	}
	log.Infof("target schema: %s", sch)

	// In PostgresSQL, connections are tightly coupled to the target
	// database.  Cross-database queries are generally unsupported, as
	// opposed to CockroachDB, which allows any database to be queried
	// from any connection.
	//
	// To resolve this, we're going to re-open the target database
	// connection so that the connection uses the schema that we have
	// just created. We also need to recreate the statement cache so
	// that it's associated with the newly-constructed database
	// connection.
	if pool.Info().Product == types.ProductPostgreSQL {
		db, _ := sch.Split()
		conn := fmt.Sprintf("%s/%s", pool.ConnectionString, db.Raw())
		next, err := stdpool.OpenPgxAsTarget(ctx, conn,
			stdpool.WithDiagnostics(diags, "target_reopened"))
		if err != nil {
			return sinktest.TargetSchema{}, err
		}

		nextCache := ProvideTargetStatements(ctx, next)
		pool.ConnectionString = conn
		pool.DB = next.DB
		stmts.Cache = nextCache.Cache
	} else if pool.Info().Product == types.ProductOracle {
		// Similar to the above, we want to reconnect as something other
		// than the system user the test stack is initialized with.
		u, err := url.Parse(pool.ConnectionString)
		if err != nil {
			return sinktest.TargetSchema{}, errors.Wrap(err, pool.ConnectionString)
		}
		u.User = url.UserPassword(sch.Raw(), DummyPassword)
		conn := u.String()

		next, err := stdpool.OpenOracleAsTarget(ctx, conn,
			stdpool.WithDiagnostics(diags, "target_reopened"))
		if err != nil {
			return sinktest.TargetSchema{}, err
		}
		nextCache := ProvideTargetStatements(ctx, next)
		pool.ConnectionString = conn
		pool.DB = next.DB
		stmts.Cache = nextCache.Cache
	}
	return ret, nil
}

// ProvideTargetStatements is called by Wire to construct a
// prepared-statement cache. Anywhere the associated TargetPool is
// reused should also reuse the cache.
func ProvideTargetStatements(ctx *stopper.Context, pool *types.TargetPool) *types.TargetStatements {
	ret := stmtcache.New[string](ctx, pool.DB, statementCacheSize)
	return &types.TargetStatements{Cache: ret}
}

func provideSchema[P types.AnyPool](
	ctx *stopper.Context, pool P, prefix string,
) (ident.Schema, error) {
	switch pool.Info().Product {
	case types.ProductCockroachDB, types.ProductMariaDB, types.ProductMySQL, types.ProductPostgreSQL:
		return CreateSchema(ctx, pool, prefix)

	case types.ProductOracle:
		// Each package tests run in a separate binary, so we need a
		// "globally" unique ID.  While PIDs do recycle, they're highly
		// unlikely to do so during a single run of the test suite.
		name := ident.New(fmt.Sprintf(
			"%s_%d_%d", strings.ToUpper(prefix), os.Getpid(), dbIdentCounter.Add(1)))

		// This syntax doesn't use a string literal, but an identifier
		// for the password.
		err := retry.Execute(ctx, pool, fmt.Sprintf(
			"CREATE USER %s IDENTIFIED BY %s", name, DummyPassword))
		if err != nil {
			return ident.Schema{}, errors.Wrapf(err, "could not create user %s", name)
		}

		err = retry.Execute(ctx, pool, fmt.Sprintf("ALTER USER %s QUOTA UNLIMITED ON USERS", name))
		if err != nil {
			return ident.Schema{}, errors.Wrapf(err, "could not grant quota to %s", name)
		}

		err = retry.Execute(ctx, pool, fmt.Sprintf(
			"GRANT CREATE SESSION, CREATE SEQUENCE, CREATE TABLE, CREATE TYPE, CREATE VIEW TO %s", name))
		if err != nil {
			return ident.Schema{}, errors.Wrapf(err, "could not grant permissions to %s", name)
		}

		ctx.Defer(func() {
			// Use background since stopper context has stopped.
			err := retry.Execute(context.Background(), pool, fmt.Sprintf("DROP USER %s CASCADE", name))
			if err != nil {
				log.WithError(err).Warnf("could not clean up schema %s", name)
			}
		})

		return ident.MustSchema(name), nil

	default:
		return ident.Schema{},
			errors.Errorf("cannot create test db for %s", pool.Info().Product)
	}
}

// Ensure unique database identifiers within a test run.
var dbIdentCounter atomic.Int32

// CreateSchema creates a schema with a unique name that will be
// dropped when the stopper has stopped.
func CreateSchema[P types.AnyPool](
	ctx *stopper.Context, pool P, prefix string,
) (ident.Schema, error) {
	dbNum := dbIdentCounter.Add(1)

	// Each package tests run in a separate binary, so we need a
	// "globally" unique ID.  While PIDs do recycle, they're highly
	// unlikely to do so during a single run of the test suite.
	// We use dashes in the name to ensure that the identifier is always
	// correctly quoted when sent in SQL commands.
	name := ident.New(fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), dbNum))

	cancel := func() {
		option := "CASCADE"
		switch pool.Info().Product {
		case types.ProductMariaDB, types.ProductMySQL:
			option = ""
		default:
			// nothing to do.
		}
		// Called from stopper.Context.Defer, so we must use Background.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := retry.Execute(ctx, pool,
			fmt.Sprintf("DROP DATABASE IF EXISTS %s %s", name, option))
		log.WithError(err).WithField("target", name).Debug("dropped database")
	}
	ctx.Defer(cancel)

	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		"CREATE DATABASE %s", name)); err != nil {
		return ident.Schema{}, errors.WithStack(err)
	}

	if pool.Info().Product == types.ProductCockroachDB {
		if err := retry.Execute(ctx, pool, fmt.Sprintf(
			`ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds = 600`, name)); err != nil {
			return ident.Schema{}, errors.WithStack(err)
		}
	}
	var sch ident.Schema
	var err error
	switch pool.Info().Product {
	case types.ProductMariaDB, types.ProductMySQL:
		sch, err = ident.NewSchema(name)
	default:
		sch, err = ident.NewSchema(name, ident.Public)
	}
	if err != nil {
		return ident.Schema{}, err
	}

	return sch, nil
}

// A global counter for allocating all temp tables in a test run. We
// know that the enclosing database has a unique name, but it's
// convenient for all test table names to be unique as well.
var tempTable atomic.Int32

// CreateTable creates a test table. The schemaSpec parameter must have
// exactly one %s substitution parameter for the database name and table
// name.
func CreateTable[P types.AnyPool](
	ctx context.Context, pool P, enclosing ident.Schema, schemaSpec string,
) (TableInfo[P], error) {
	tableNum := tempTable.Add(1)
	// We use a dash here to ensure that the table name must be
	// correctly quoted when sent as a SQL command.
	tableName := ident.New(fmt.Sprintf("tbl-%d", tableNum))
	table := ident.NewTable(enclosing, tableName)
	err := retry.Execute(ctx, pool, fmt.Sprintf(schemaSpec, table))
	return TableInfo[P]{pool, table}, errors.WithStack(err)
}
