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

// Package stdpool creates connection pool configurations.
package stdpool

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

const (
	defaultMaxLifetime       = 10 * time.Minute
	defaultMaxLifetimeJitter = time.Minute
	defaultMinConns          = 1
	defaultMaxConns          = 128
)

// ParseConfig parses a pgxpool.Config with common defaults.
//   - application_name=cdc-sink, if unset.
//   - max connection lifetime is 10 minutes.
//   - pool min size is set to 1
//   - pool max size is set to at least 128
//   - success and latency metrics for creating connections
func ParseConfig(connectString string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(connectString)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse %q", connectString)
	}
	// Identify traffic.
	if _, found := cfg.ConnConfig.RuntimeParams["application_name"]; !found {
		cfg.ConnConfig.RuntimeParams["application_name"] = "cdc-sink"
	}

	// Ensure connection diversity through long-lived loadbalancers.
	if cfg.MaxConnLifetime == 0 {
		cfg.MaxConnLifetime = defaultMaxLifetime
	}
	if cfg.MaxConnLifetimeJitter == 0 {
		cfg.MaxConnLifetimeJitter = defaultMaxLifetimeJitter
	}
	// Add reasonable bounds to pool size.
	cfg.MinConns = defaultMinConns
	if cfg.MaxConns < defaultMaxConns {
		cfg.MaxConns = defaultMaxConns
	}

	// Provide metrics around database connections. We use a standard
	// net.Dialer (per defaults from pgx) and measure both how long it
	// takes to create the network connection and how long it takes to
	// have a fully-functioning SQL connection.  This would allow users
	// to distinguish between dis-function in the network or the target
	// database.
	var dialer = &net.Dialer{
		KeepAlive: 5 * time.Minute,
		Timeout:   10 * time.Second,
	}
	hName := fmt.Sprintf("%s:%d", cfg.ConnConfig.Host, cfg.ConnConfig.Port)
	dialErrors := poolDialErrors.WithLabelValues(hName)
	dialLatency := poolDialLatency.WithLabelValues(hName)
	dialSuccesses := poolDialSuccesses.WithLabelValues(hName)
	readyLatency := poolReadyLatency.WithLabelValues(hName)
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()
		conn, err := dialer.DialContext(ctx, network, addr)
		if err == nil {
			dialSuccesses.Inc()
			dialLatency.Observe(time.Since(start).Seconds())
			conn = &timedConn{conn, start}
		} else {
			dialErrors.Inc()
		}
		return conn, err
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if timed, ok := conn.PgConn().Conn().(*timedConn); ok {
			readyLatency.Observe(time.Since(timed.start).Seconds())
		}
		return nil
	}

	return cfg, nil
}

type timedConn struct {
	net.Conn
	start time.Time
}
