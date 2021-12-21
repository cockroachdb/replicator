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
	"log"
	"math/rand"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// DBInfo encapsulates metadata and a connection to a database.
type DBInfo struct {
	db      *pgxpool.Pool
	version string
}

// CreateDB creates a new testing SQL DATABASE whose lifetime is bounded
// by that of the associated context, which must be derived from the
// Context() method in this package.
func CreateDB(ctx context.Context) (dbName ident.Ident, cancel func(), _ error) {
	dbInfo := DB(ctx)
	pool := dbInfo.Pool()
	dbNum := rand.Intn(10000)
	name := ident.New(fmt.Sprintf("_test_db_%d", dbNum))

	cancel = func() {
		err := retry.Execute(ctx, pool, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", name))
		log.Printf("dropped database %s %v", name, err)
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
