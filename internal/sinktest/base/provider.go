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
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	connString = flag.String("testConnect",
		"postgresql://root@localhost:26257/defaultdb?sslmode=disable",
		"the connection string to use for testing")

	targetString = flag.String("testTarget", "", "use an alternate connection string for the target database")
)

// TestSet is used by wire.
var TestSet = wire.NewSet(
	ProvideContext,
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
	Context     context.Context    // The context for the test.
	StagingPool *types.StagingPool // Access to __cdc_sink database.
	TargetPool  *types.TargetPool  // Access to the destination.
	StagingDB   ident.StagingDB    // The _cdc_sink SQL DATABASE.
	TestDB      TestDB             // A unique SQL DATABASE identifier.
}

// A global counter for allocating all temp tables in a test run. We
// know that the enclosing database has a unique name, but it's
// convenient for all test table names to be unique as well.
var tempTable int32

// CreateTable creates a test table within the TestDB. The
// schemaSpec parameter must have exactly one %s substitution parameter
// for the database name and table name.
func (f *Fixture) CreateTable(
	ctx context.Context, schemaSpec string,
) (TableInfo[*types.TargetPool], error) {
	tableNum := atomic.AddInt32(&tempTable, 1)
	tableName := ident.New(fmt.Sprintf("tbl_%d", tableNum))
	table := ident.NewTable(f.TestDB.Schema(), tableName)

	err := retry.Execute(ctx, f.TargetPool, fmt.Sprintf(schemaSpec, table))
	return TableInfo[*types.TargetPool]{f.TargetPool, table}, errors.WithStack(err)
}

var caseTimout = flag.Duration(
	"caseTimout",
	2*time.Minute,
	"raise this value when debugging to allow individual tests to run longer",
)

// ProvideContext returns an execution context that is associated with a
// singleton connection to a CockroachDB cluster.
func ProvideContext() (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), *caseTimout)
	return ctx, cancel, nil
}

// ProvideStagingDB create a globally-unique SQL database. The cancel
// function will drop the database.
func ProvideStagingDB(
	ctx context.Context, pool *types.StagingPool,
) (ident.StagingDB, func(), error) {
	ret, cancel, err := CreateSchema(ctx, pool, "_cdc_sink")
	return ident.StagingDB(ret), cancel, err
}

// ProvideStagingPool opens a connection to the CockroachDB staging
// cluster under test. This function will also configure the rangefeed
// and license cluster settings if they have not been previously
// configured.
func ProvideStagingPool(ctx context.Context) (*types.StagingPool, func(), error) {
	ret, cancel, err := stdpool.OpenPgxAsStaging(ctx, *connString)
	if err != nil {
		return nil, nil, err
	}

	success := false
	defer func() {
		if !success {
			cancel()
		}
	}()

	// Set the cluster settings once, if we need to.
	var enabled bool
	if err := retry.Retry(ctx, func(ctx context.Context) error {
		return ret.QueryRow(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled)
	}); err != nil {
		return nil, nil, errors.Wrap(err, "could not check cluster setting")
	}
	if !enabled {
		if lic, ok := os.LookupEnv("COCKROACH_DEV_LICENSE"); ok {
			if err := retry.Execute(ctx, ret,
				"SET CLUSTER SETTING cluster.organization = $1",
				"Cockroach Labs - Production Testing",
			); err != nil {
				return nil, nil, errors.Wrap(err, "could not set cluster.organization")
			}
			if err := retry.Execute(ctx, ret,
				"SET CLUSTER SETTING enterprise.license = $1", lic,
			); err != nil {
				return nil, nil, errors.Wrap(err, "could not set enterprise.license")
			}
		}

		if err := retry.Execute(ctx, ret,
			"SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
			return nil, nil, errors.Wrap(err, "could not enable rangefeeds")
		}
	}

	success = true
	return ret, cancel, nil
}

// ProvideTargetPool connects to the target database (which is most
// often the same as the staging database).
func ProvideTargetPool(ctx context.Context) (*types.TargetPool, func(), error) {
	target := *targetString
	if target == "" {
		target = *connString
	}
	return stdpool.OpenTarget(ctx, target)
}

// ProvideTestDB create a globally-unique SQL database. The cancel
// function will drop the database.
func ProvideTestDB(ctx context.Context, pool *types.TargetPool) (TestDB, func(), error) {
	switch pool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		ret, cancel, err := CreateSchema(ctx, pool, "_test_db")
		return TestDB(ret), cancel, err

	case types.ProductOracle:
		// Each package tests run in a separate binary, so we need a
		// "globally" unique ID.  While PIDs do recycle, they're highly
		// unlikely to do so during a single run of the test suite.
		name := ident.New(fmt.Sprintf("target_%d_%d", os.Getpid(), atomic.AddInt32(&dbIdentCounter, 1)))

		_, err := pool.ExecContext(ctx, fmt.Sprintf("CREATE USER %s", name))
		if err != nil {
			return TestDB{}, nil, errors.Wrapf(err, "could not create user %s", name)
		}

		cancel := func() {
			_, err := pool.ExecContext(ctx, fmt.Sprintf("DROP USER %s CASCADE", name))
			if err != nil {
				log.WithError(err).Warnf("could not clean up schema %s", name)
			}
		}

		return TestDB(ident.MustSchema(name)), cancel, nil

	default:
		return *new(TestDB), nil, errors.Errorf("cannot create test db for %s", pool.Product)
	}
}

// Ensure unique database identifiers within a test run.
var dbIdentCounter int32

// CreateSchema creates a schema with a unique name that will be
// dropped when the cancel function is called.
func CreateSchema[P types.AnyPool](
	ctx context.Context, pool P, prefix string,
) (ident.Schema, func(), error) {
	dbNum := atomic.AddInt32(&dbIdentCounter, 1)

	// Each package tests run in a separate binary, so we need a
	// "globally" unique ID.  While PIDs do recycle, they're highly
	// unlikely to do so during a single run of the test suite.
	name := ident.New(fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), dbNum))

	cancel := func() {
		err := retry.Execute(ctx, pool, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", name))
		log.WithError(err).WithField("target", name).Debug("dropped database")
	}

	// Clean up if anything below fails.
	success := false
	defer func() {
		if !success {
			cancel()
		}
	}()

	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		"CREATE DATABASE %s", name)); err != nil {
		return ident.Schema{}, cancel, errors.WithStack(err)
	}

	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		`ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds = 600`, name)); err != nil {
		return ident.Schema{}, cancel, errors.WithStack(err)
	}

	sch, err := ident.NewSchema(name, ident.Public)
	if err != nil {
		return ident.Schema{}, cancel, err
	}

	success = true
	return sch, cancel, nil
}

// TestDB is an injection point that holds the name of a unique database
// schema in which to store user data.
type TestDB ident.Schema

// Schema returns the underlying database identifier.
func (t TestDB) Schema() ident.Schema { return ident.Schema(t) }
