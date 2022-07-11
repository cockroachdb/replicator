// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package stdpool creates connection pool configurations.
package stdpool

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

const (
	defaultMaxLifetime = 10 * time.Minute
	defaultMinConns    = 1
	defaultMaxConns    = 128
)

// ParseConfig parses a pgxpool.Config with common defaults.
//  * application_name=cdc-sink, if unset.
//  * max connection lifetime is 10 minutes.
//  * pool min size is set to 1
//  * pool max size is set to at least 128
//  * success and latency metrics for creating connections
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
	cfg.MaxConnLifetime = defaultMaxLifetime
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
