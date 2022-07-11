// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/memo"
	"github.com/cockroachdb/cdc-sink/internal/util/serial"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideLoop,
	ProvidePool,
	ProvideQuerier,
	ProvideSerializer,
	ProvideStagingDB,
)

// ProvideLoop is called by Wire to create the replication loop. This
// function starts a background goroutine, which will be terminated
// by the cancel function.
func ProvideLoop(
	ctx context.Context,
	config *Config,
	dialect Dialect,
	fans *fan.Fans,
	pool *pgxpool.Pool,
	serializer *serial.Pool,
	stagingDB ident.StagingDB,
) (*Loop, func(), error) {
	if config.ChaosProb > 0 {
		dialect = WithChaos(dialect, config.ChaosProb)
	}

	loop := &loop{
		consistentPointKey: config.ConsistentPointKey,
		dialect:            dialect,
		retryDelay:         config.RetryDelay,
		serializer:         serializer,
		standbyDeadline:    time.Now().Add(standbyTimeout),
		stopped:            make(chan struct{}),
		targetDB:           config.TargetDB,
		targetPool:         pool,
	}
	loop.consistentPointUpdated = sync.NewCond(&loop.mu)

	if config.ConsistentPointKey != "" {
		var err error
		loop.memo, err = memo.New(ctx, pool, ident.NewTable(stagingDB.Ident(), ident.Public, ident.New("memo")))
		if err != nil {
			return nil, nil, errors.Wrap(err, "could not create memo table")
		}
		cp, err := loop.retrieveConsistentPoint(ctx, loop.memo,
			config.ConsistentPointKey,
			[]byte(config.DefaultConsistentPoint))
		if err != nil {
			return nil, nil, errors.Wrap(err, "could not restore consistentPoint")
		}
		loop.mu.consistentPoint, err = dialect.UnmarshalStamp(cp)
		if err != nil {
			return nil, nil, errors.Wrap(err, "could not restore consistentPoint")
		}
	}

	applyShards := 16
	if serializer != nil {
		applyShards = 1
	}

	var cancelFan func()
	var err error
	loop.fan, cancelFan, err = fans.New(
		config.ApplyTimeout,
		loop.setConsistentPoint,
		applyShards,
		config.BytesInFlight)
	if err != nil {
		return nil, nil, err
	}

	// The loop runs in a background context so that we have better
	// control over the lifecycle.
	loopCtx, cancel := context.WithCancel(context.Background())
	go func() {
		loop.run(loopCtx)
		cancelFan()
		close(loop.stopped)
	}()

	return &Loop{loop}, cancel, nil
}

// ProvidePool is called by Wire to create a connection pool that
// accesses the target cluster. The pool will be closed by the cancel
// function.
func ProvidePool(ctx context.Context, config *Config) (*pgxpool.Pool, func(), error) {
	if err := config.Preflight(); err != nil {
		return nil, nil, err
	}

	// Bring up connection to target database.
	targetCfg, err := stdpool.ParseConfig(config.TargetConn)
	if err != nil {
		return nil, nil, err
	}
	targetPool, err := pgxpool.ConnectConfig(ctx, targetCfg)
	cancelMetrics := stdpool.PublishMetrics(targetPool)
	return targetPool, func() {
		cancelMetrics()
		targetPool.Close()
	}, errors.Wrap(err, "could not connect to CockroachDB")
}

// ProvideQuerier is called by Wire. If we're running in serial (i.e.
// not immediate) mode, the serializer will be use by downstream
// components.
func ProvideQuerier(pool *pgxpool.Pool, serializer *serial.Pool) pgxtype.Querier {
	if serializer == nil {
		return pool
	}
	return serializer
}

// ProvideSerializer is called by Wire. This function will return nil
// if the configuration is in immediate mode.
func ProvideSerializer(config *Config, pool *pgxpool.Pool) *serial.Pool {
	if config.Immediate {
		return nil
	}
	return &serial.Pool{Pool: pool}
}

// ProvideStagingDB is called by Wire to retrieve the name of the
// _cdc_sink SQL DATABASE.
func ProvideStagingDB(config *Config) (ident.StagingDB, error) {
	if err := config.Preflight(); err != nil {
		return ident.StagingDB{}, err
	}
	return ident.StagingDB(config.StagingDB), nil
}
