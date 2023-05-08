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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

var connString = flag.String("testConnect",
	"postgresql://root@localhost:26257/defaultdb?sslmode=disable",
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
	globalDBInfo.DBInfo = &DBInfo{}

	if !flag.Parsed() {
		flag.Parse()
	}

	// Create the testing database
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cfg, err := stdpool.ParseConfig(*connString)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not open database connection")
	}

	if err := retry.Retry(ctx, func(ctx context.Context) error {
		return pool.QueryRow(ctx, "SELECT version()").Scan(&globalDBInfo.version)
	}); err != nil {
		return nil, errors.Wrap(err, "could not determine cluster version")
	}

	// Reset the pool to one that enables the hash-sharded feature in
	// older versions of CockroachDB.
	if strings.Contains(globalDBInfo.version, "v20.") || strings.Contains(globalDBInfo.version, "v21.") {
		cfg := cfg.Copy()
		cfg.ConnConfig.RuntimeParams["experimental_enable_hash_sharded_indexes"] = "true"

		pool.Close()
		pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return nil, errors.Wrap(err, "could not re-open pool")
		}
	}

	globalDBInfo.db = pool

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

	return globalDBInfo.DBInfo, nil
}

// GetRowCount returns the number of rows in the table.
func GetRowCount(ctx context.Context, db types.Querier, name ident.Table) (int, error) {
	var count int
	err := retry.Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&count)
	})
	return count, err
}
