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
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideFactory,
	ProvideLoop,
	ProvidePool,
	ProvideStagingDB,
	wire.Bind(new(pgxtype.Querier), new(*pgxpool.Pool)),
)

// ProvideFactory returns a utility which can create multiple logical
// loops.
func ProvideFactory(
	appliers types.Appliers, config *Config, fans *fan.Fans, memo types.Memo, pool *pgxpool.Pool,
) (*Factory, func()) {
	f := &Factory{
		appliers: appliers,
		cfg:      config,
		fans:     fans,
		memo:     memo,
		pool:     pool,
	}
	f.mu.loops = make(map[string]*Loop)
	return f, f.Close
}

// ProvideLoop is called by Wire to create the replication loop. This
// function starts a background goroutine, which will be terminated
// by the cancel function.
func ProvideLoop(
	ctx context.Context,
	appliers types.Appliers,
	config *Config,
	dialect Dialect,
	fans *fan.Fans,
	memo types.Memo,
	pool *pgxpool.Pool,
) (*Loop, func(), error) {
	var err error
	if config.ChaosProb > 0 {
		dialect = WithChaos(dialect, config.ChaosProb)
	}
	loop := &loop{
		config:          config,
		dialect:         dialect,
		memo:            memo,
		standbyDeadline: time.Now().Add(config.StandbyTimeout),
		stopped:         make(chan struct{}),
		targetPool:      pool,
	}
	loop.consistentPoint.Cond = sync.NewCond(&sync.Mutex{})
	loop.consistentPoint.stamp, err = loop.loadConsistentPoint(ctx)
	if err != nil {
		return nil, nil, err
	}

	loop.events.fan = (&metricsEvents{Events: &fanEvents{
		State:  loop,
		config: config,
		fans:   fans,
	}}).withLoopName(config.LoopName)

	loop.events.serial = (&metricsEvents{Events: &serialEvents{
		State:    loop,
		appliers: appliers,
		pool:     pool,
	}}).withLoopName(config.LoopName)

	// The loop runs in a background context so that we have better
	// control over the lifecycle.
	loopCtx, cancel := context.WithCancel(context.Background())
	go func() {
		loop.run(loopCtx)
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

// ProvideStagingDB is called by Wire to retrieve the name of the
// _cdc_sink SQL DATABASE.
func ProvideStagingDB(config *Config) (ident.StagingDB, error) {
	if err := config.Preflight(); err != nil {
		return ident.StagingDB{}, err
	}
	return ident.StagingDB(config.StagingDB), nil
}
