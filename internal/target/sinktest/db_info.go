// Copyright 2021 The Cockroach Authors.
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
	"os"
	"sync/atomic"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// DBInfo encapsulates metadata and a connection to a database.
type DBInfo struct {
	db      *pgxpool.Pool
	version string
}

// Ensure unique database identifiers within a test run.
var dbIdentCounter int32

// CreateDB creates a new testing SQL DATABASE whose lifetime is bounded
// by that of the associated context, which must be derived from the
// Context() method in this package.
func CreateDB(ctx context.Context) (dbName ident.Ident, cancel func(), _ error) {
	dbInfo := DB(ctx)
	pool := dbInfo.Pool()
	dbNum := atomic.AddInt32(&dbIdentCounter, 1)

	// Each package tests run in a separate binary, so we need a
	// "globally" unique ID.  While PIDs do recycle, they're highly
	// unlikely to do so during a single run of the test suite.
	name := ident.New(fmt.Sprintf("_test_db_%d_%d", os.Getpid(), dbNum))

	cancel = func() {
		err := retry.Execute(ctx, pool, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", name))
		log.WithError(err).WithField("target", name).Debug("dropped database")
	}

	// Ensure that the base database exists
	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS %s", ident.StagingDB)); err != nil {
		return name, cancel, errors.WithStack(err)
	}

	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS %s", name)); err != nil {
		return name, cancel, errors.WithStack(err)
	}

	if err := retry.Execute(ctx, pool, fmt.Sprintf(
		`ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds = 600`, name)); err != nil {
		return name, cancel, errors.WithStack(err)
	}

	return name, cancel, nil
}

// Pool returns the underlying database connection.
func (di DBInfo) Pool() *pgxpool.Pool { return di.db }

// Version returns the database version.
func (di DBInfo) Version() string { return di.version }
