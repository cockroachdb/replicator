// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sinktest contains code to assist in writing tests.
package sinktest

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

var connString = flag.String("testConnect",
	"postgresql://root@localhost:26257/defaultdb?sslmode=disable&experimental_enable_hash_sharded_indexes=true",
	"the connection string to use for testing")

var globalDBInfo struct {
	sync.Mutex
	*DBInfo
}

func bootstrap(ctx context.Context) (*DBInfo, error) {
	globalDBInfo.Lock()
	defer globalDBInfo.Unlock()

	if globalDBInfo.DBInfo != nil {
		return globalDBInfo.DBInfo, nil
	}

	if !flag.Parsed() {
		flag.Parse()
	}

	// Create the testing database
	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pool, err := pgxpool.Connect(ctx, *connString)
	if err != nil {
		return nil, errors.Wrap(err, "could not open database connection")
	}
	globalDBInfo.DBInfo = &DBInfo{db: pool}

	if lic, ok := os.LookupEnv("COCKROACH_DEV_LICENSE"); ok {
		if err := retry.Execute(ctx, pool,
			"SET CLUSTER SETTING cluster.organization = $1",
			"Cockroach Labs - Production Testing",
		); err != nil {
			return nil, errors.Wrap(err, "could not set cluster.organization")
		}
		if err := retry.Execute(ctx, pool,
			"SET CLUSTER SETTING enterprise.license = $1", lic,
		); err != nil {
			return nil, errors.Wrap(err, "could not set enterprise.license")
		}
	}

	if err := retry.Execute(ctx, pool,
		"SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		return nil, errors.Wrap(err, "could not enable rangefeeds")
	}

	if err := retry.Retry(ctx, func(ctx context.Context) error {
		return pool.QueryRow(ctx, "SELECT version()").Scan(&globalDBInfo.version)
	}); err != nil {
		return nil, errors.Wrap(err, "could not determine cluster version")
	}

	return globalDBInfo.DBInfo, nil
}

// CreateDB creates a new testing SQL DATABASE whose lifetime is bounded
// by that of the associated context, which must be derived from the
// Context() method in this package.
func CreateDB(ctx context.Context) (dbName ident.Ident, cancel func(), _ error) {
	db := DB(ctx).Pool()
	dbNum := rand.Intn(10000)
	name := ident.New(fmt.Sprintf("_test_db_%d", dbNum))

	cancel = func() {
		err := retry.Execute(ctx, db, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", name))
		log.Printf("dropped database %s %v", name, err)
	}

	// Ensure that the base database exists
	if err := retry.Execute(ctx, db, fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS %s", ident.StagingDB)); err != nil {
		return name, cancel, errors.WithStack(err)
	}

	if err := retry.Execute(ctx, db, fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS %s", name)); err != nil {
		return name, cancel, errors.WithStack(err)
	}

	if err := retry.Execute(ctx, db, fmt.Sprintf(
		`ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds = 600`, name)); err != nil {
		return name, cancel, errors.WithStack(err)
	}

	return name, cancel, nil
}

// CreateTable creates a test table and returns a unique name. The
// schemaSpec parameter must have exactly one %s substitution parameter
// for the database name and table name.
func CreateTable(ctx context.Context, dbName ident.Ident, schemaSpec string) (TableInfo, error) {
	var table ident.Table
	db := DB(ctx)
	if db == nil {
		return TableInfo{}, errors.New("no database in context")
	}

outer:
	for {
		// Create the testing database
		tableNum := rand.Intn(10000)
		tableName := ident.New(fmt.Sprintf("_test_table_%d", tableNum))

		// Find the DB.
		var actualTableName string
		err := retry.Retry(ctx, func(ctx context.Context) error {
			return db.Pool().QueryRow(ctx,
				fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = $1", dbName),
				tableName.Raw(),
			).Scan(&actualTableName)
		})
		switch err {
		case pgx.ErrNoRows:
			table = ident.NewTable(dbName, ident.Public, tableName)
			break outer
		case nil:
			continue
		default:
			return TableInfo{}, errors.WithStack(err)
		}
	}

	err := retry.Execute(ctx, db.Pool(), fmt.Sprintf(schemaSpec, table))
	return TableInfo{db, table}, errors.WithStack(err)
}

// GetRowCount returns the number of rows in the table.
func GetRowCount(ctx context.Context, db pgxtype.Querier, name ident.Table) (int, error) {
	var count int
	err := retry.Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&count)
	})
	return count, err
}
