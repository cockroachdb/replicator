// Copyright 2024 The Cockroach Authors
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

package sinkprod

import (
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/cockroachdb/cdc-sink/internal/util/stmtcache"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// TargetConfig defines target-database connection behaviors.
type TargetConfig struct {
	// The maximum length of time to wait for an incoming transaction
	// to settle (i.e. to detect stalls in the target database).
	ApplyTimeout time.Duration
	// Connection string for the target cluster.
	Conn string
	// The maximum lifetime for a database connection; improves
	// loadbalancer compatibility.
	Lifetime time.Duration
	// The number of connections to the target database. If zero, a
	// default value will be used.
	PoolSize int
	// The number of prepared statements to retain in the target
	// database connection pool. Depending on the database in question,
	// there may be more or fewer available resources to retain
	// statements.
	StatementCacheSize int
}

// Bind adds flags to the set.
func (c *TargetConfig) Bind(f *pflag.FlagSet) {
	f.DurationVar(&c.ApplyTimeout, "applyTimeout", defaultApplyTimeout,
		"the maximum amount of time to wait for an update to be applied")
	f.StringVar(&c.Conn, "targetConn", "",
		"the target database's connection string; always required")
	f.IntVar(&c.PoolSize, "targetDBConns", defaultPoolSize,
		"the maximum pool size to the target cluster")
	f.DurationVar(&c.Lifetime, "targetDBLifetime", defaultLifetime,
		"the maximum lifetime for an individual database connection")
	f.IntVar(&c.StatementCacheSize, "targetStatementCacheSize", defaultCacheSize,
		"the maximum number of prepared statements to retain")
}

// Preflight ensures that unset configuration options have sane defaults
// and returns an error if the TargetConfig is missing any fields for which a
// default cannot be provided.
func (c *TargetConfig) Preflight() error {
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = defaultApplyTimeout
	}
	if c.Conn == "" {
		return errors.New("targetConn must be set")
	}
	if c.Lifetime == 0 {
		c.Lifetime = defaultLifetime
	}
	if c.PoolSize == 0 {
		c.PoolSize = defaultPoolSize
	}
	if c.StatementCacheSize == 0 {
		c.StatementCacheSize = defaultCacheSize
	}
	return nil
}

// ProvideTargetPool is called by Wire to create a connection pool that
// accesses the target cluster. The pool will be closed when the context
// is stopped.
func ProvideTargetPool(
	ctx *stopper.Context, config *TargetConfig, diags *diag.Diagnostics,
) (*types.TargetPool, error) {
	options := []stdpool.Option{
		stdpool.WithConnectionLifetime(config.Lifetime),
		stdpool.WithDiagnostics(diags, "target"),
		stdpool.WithMetrics("target"),
		stdpool.WithPoolSize(config.PoolSize),
		stdpool.WithTransactionTimeout(config.ApplyTimeout),
	}

	ret, err := stdpool.OpenTarget(ctx, config.Conn, options...)
	if err != nil {
		return nil, err
	}
	ctx.Defer(func() { _ = ret.Close() })

	return ret, err
}

// ProvideStatementCache is called by Wire to construct a
// prepared-statement cache. Anywhere the associated TargetPool is
// reused should also reuse the cache.
func ProvideStatementCache(
	config *TargetConfig, pool *types.TargetPool, diags *diag.Diagnostics,
) (*types.TargetStatements, error) {
	ret := stmtcache.New[string](pool.DB, config.StatementCacheSize)
	if err := diags.Register("targetStatements", ret); err != nil {
		return nil, err
	}
	return &types.TargetStatements{Cache: ret}, nil
}
