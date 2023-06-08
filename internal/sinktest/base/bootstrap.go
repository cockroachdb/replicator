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

package base

import (
	"context"
	"flag"
	"os"
	"strings"
	"sync"
	"time"

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
