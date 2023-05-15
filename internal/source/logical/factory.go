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

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// Factory supports uses cases where it is desirable to have multiple,
// independent logical loops that share common resources.
type Factory struct {
	appliers   types.Appliers
	cfg        Config
	memo       types.Memo
	pool       *pgxpool.Pool
	watchers   types.Watchers
	userscript *script.UserScript

	mu struct {
		sync.Mutex
		loops map[string]*Loop
	}
}

// Close terminates all running loops and waits for them to shut down.
func (f *Factory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Request stopping.
	for _, facade := range f.mu.loops {
		facade.loop.running.Stop(f.cfg.Base().ApplyTimeout)
	}
	// Wait for shutdown.
	for key, facade := range f.mu.loops {
		<-facade.loop.running.Done()
		delete(f.mu.loops, key)
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
		case <-found.loop.running.Stopping():
			// Re-create the stopped loop.
		default:
			return found, nil
		}
	}

	loop, err := f.newLoop(ctx, config, dialect)
	if err != nil {
		return nil, err
	}

	f.mu.loops[name] = loop
	go loop.loop.run()

	return loop, nil
}

// newLoop constructs a loop, but does not start or memoize it.
func (f *Factory) newLoop(ctx context.Context, config *BaseConfig, dialect Dialect) (*Loop, error) {
	watcher, err := f.watchers.Get(ctx, config.TargetDB)
	if err != nil {
		return nil, err
	}
	if config.ChaosProb > 0 {
		dialect = WithChaos(dialect, config.ChaosProb)
	}
	loop := &loop{
		config:     config,
		dialect:    dialect,
		factory:    f,
		memo:       f.memo,
		running:    stopper.WithContext(ctx),
		targetPool: f.pool,
	}
	loop.consistentPoint.updated = make(chan struct{})
	initialPoint, err := loop.loadConsistentPoint(ctx)
	if err != nil {
		return nil, err
	}
	loop.consistentPoint.stamp = initialPoint

	loop.events.fan = &fanEvents{
		loop: loop,
	}

	loop.events.serial = &serialEvents{
		appliers: f.appliers,
		loop:     loop,
		pool:     f.pool,
	}

	if config.ForeignKeysEnabled {
		loop.events.fan = &orderedEvents{
			Events:  loop.events.fan,
			Watcher: watcher,
		}
		loop.events.serial = &orderedEvents{
			Events:  loop.events.serial,
			Watcher: watcher,
		}
	} else {
		// Sanity-check that there are no FKs defined.
		if len(watcher.Get().Order) > 1 {
			return nil, errors.New("the destination database has tables with foreign keys, " +
				"but support for FKs is not enabled")
		}
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
