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

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/jackc/pgx/v4/pgxpool"
)

type loopCancel struct {
	loop   *Loop
	cancel func()
}

// Factory supports uses cases where it is desirable to have multiple,
// independent logical loops that share common resources.
type Factory struct {
	appliers   types.Appliers
	cfg        Config
	fans       *fan.Fans
	memo       types.Memo
	pool       *pgxpool.Pool
	userscript *script.UserScript

	mu struct {
		sync.Mutex
		loops map[string]*loopCancel
	}
}

// Close terminates all running loops and waits for them to shut down.
func (f *Factory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, hook := range f.mu.loops {
		// These are just context-cancellations functions that return
		// immediately, so we need to wait for the loops to stop below.
		hook.cancel()
	}
	for _, hook := range f.mu.loops {
		<-hook.loop.Stopped()
	}
}

// Get constructs or retrieves the named Loop.
func (f *Factory) Get(ctx context.Context, dialect Dialect, options ...Option) (*Loop, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	config := f.cfg.Base().Copy()
	for _, option := range options {
		option(config)
	}

	if err := config.Preflight(); err != nil {
		return nil, err
	}

	name := config.LoopName
	if found, ok := f.mu.loops[name]; ok {
		select {
		case <-found.loop.Stopped():
			// Re-create the stopped loop.
		default:
			return found.loop, nil
		}
	}

	loop, err := f.newLoop(ctx, config, dialect)
	if err != nil {
		return nil, err
	}

	// The loop runs in a background context so that we have better
	// control over the lifecycle.
	loopCtx, cancel := context.WithCancel(context.Background())

	// Add the loop to the memo map and start a goroutine to clean up.
	hook := &loopCancel{loop, cancel}
	f.mu.loops[name] = hook
	go func() {
		<-loop.Stopped()
		f.mu.Lock()
		defer f.mu.Unlock()
		// Only delete the entry that we created.
		if f.mu.loops[name] == hook {
			delete(f.mu.loops, name)
		}
	}()

	// Start the replication loop.
	go loop.loop.run(loopCtx)

	return loop, nil
}

// newLoop constructs a loop, but does not start or memoize it.
func (f *Factory) newLoop(ctx context.Context, config *BaseConfig, dialect Dialect) (*Loop, error) {
	var err error
	if config.ChaosProb > 0 {
		dialect = WithChaos(dialect, config.ChaosProb)
	}
	loop := &loop{
		config:          config,
		dialect:         dialect,
		factory:         f,
		memo:            f.memo,
		standbyDeadline: time.Now().Add(config.StandbyTimeout),
		stopped:         make(chan struct{}),
		targetPool:      f.pool,
	}
	loop.consistentPoint.Cond = sync.NewCond(&sync.Mutex{})
	initialPoint, err := loop.loadConsistentPoint(ctx)
	if err != nil {
		return nil, err
	}
	loop.consistentPoint.stamp = initialPoint

	loop.events.fan = &fanEvents{
		config: config,
		fans:   f.fans,
		loop:   loop,
	}

	loop.events.serial = &serialEvents{
		appliers: f.appliers,
		loop:     loop,
		pool:     f.pool,
	}

	// Apply logic and configurations defined by the user-script.
	if len(f.userscript.Sources) > 0 || len(f.userscript.Targets) > 0 {
		loop.events.fan = &scriptEvents{
			Events: loop.events.fan,
			Script: f.userscript,
		}
		loop.events.serial = &scriptEvents{
			Events: loop.events.serial,
			Script: f.userscript,
		}
	}

	loop.events.fan = (&metricsEvents{Events: loop.events.fan}).withLoopName(config.LoopName)
	loop.events.serial = (&metricsEvents{Events: loop.events.serial}).withLoopName(config.LoopName)

	loop.metrics.backfillStatus = backfillStatus.WithLabelValues(config.LoopName)

	return &Loop{loop, initialPoint}, nil
}
