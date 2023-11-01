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

package logical

import (
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
)

// Factory supports uses cases where it is desirable to have multiple,
// independent logical loops that share common resources.
type Factory struct {
	appliers     types.Appliers
	applyConfigs *applycfg.Configs
	baseConfig   *BaseConfig
	diags        *diag.Diagnostics
	memo         types.Memo
	scriptLoader *script.Loader
	stagingPool  *types.StagingPool
	stop         *stopper.Context
	targetPool   *types.TargetPool
	watchers     types.Watchers
}

// Immediate supports use cases where it is desirable to write directly
// into the target schema.
func (f *Factory) Immediate(target ident.Schema) (Batcher, error) {
	// Construct a fake loop and then steal the parts of the
	// implementation that are useful. We want to build the Batcher that
	// is returned by this method using the same code-path that we would
	// use to construct any other loop. There's a non-trivial amount of
	// conditional wiring needed to set up the processing pipeline. It
	// seems like there's less risk of behavioral drift over time
	// between immediate and transactional modes if we only ever use a
	// single newLoop() method.
	//
	// An alternate approach that could be taken is to write an
	// ImmediateDialect that has ReadInto and Process methods that are
	// no-ops and wait to exit. The implementation of Process would make
	// the Events / Batcher available externally. This has the downside
	// of actually needing to start the loop goroutines.
	fake, err := f.newLoop(&LoopConfig{
		Dialect:      &fakeDialect{},
		LoopName:     fmt.Sprintf("immediate-%s", target.Raw()),
		TargetSchema: target,
	})
	if err != nil {
		return nil, err
	}

	if f.baseConfig.Immediate {
		return fake.loop.events.fan, nil
	}
	return fake.loop.events.serial, nil
}

// Start constructs a new replication Loop.
func (f *Factory) Start(config *LoopConfig) (*Loop, error) {
	var err error

	// Ensure the configuration is set up and validated.
	config, err = f.expandConfig(config)
	if err != nil {
		return nil, err
	}

	// Construct the new loop and start it.
	loop, err := f.newLoop(config)
	if err != nil {
		return nil, err
	}
	f.stop.Go(func() error {
		loop.loop.run()
		return nil
	})

	return loop, nil
}

// expandConfig returns a preflighted copy of the configuration.
func (f *Factory) expandConfig(config *LoopConfig) (*LoopConfig, error) {
	config = config.Copy()

	// This sanity-checks the configured schema against the product. For
	// Cockroach and Postgres, we'll add any missing "public" schema
	// names.
	var err error
	config.TargetSchema, err = f.targetPool.Product.ExpandSchema(config.TargetSchema)
	if err != nil {
		return nil, err
	}

	return config, config.Preflight()
}

// newLoop constructs a loop, but does not start it.
func (f *Factory) newLoop(config *LoopConfig) (*Loop, error) {
	watcher, err := f.watchers.Get(config.TargetSchema)
	if err != nil {
		return nil, err
	}
	config = config.Copy()
	config.Dialect = WithChaos(config.Dialect, f.baseConfig.ChaosProb)
	loop := &loop{
		factory:    f,
		loopConfig: config,
		running:    f.stop,
	}
	initialPoint, err := loop.loadConsistentPoint(f.stop)
	if err != nil {
		return nil, err
	}
	loop.consistentPoint.Set(initialPoint)

	loop.events.fan = &fanEvents{
		loop: loop,
	}

	loop.events.serial = &serialEvents{
		appliers:   f.appliers,
		loop:       loop,
		targetPool: f.targetPool,
	}

	if f.baseConfig.ForeignKeysEnabled {
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

	// Create a branch in the diagnostics reporting for the loop.
	loopDiags, err := f.diags.Wrap(config.LoopName)
	if err != nil {
		return nil, err
	}
	// Unregister the loop on shutdown.
	f.stop.Go(func() error {
		<-f.stop.Stopping()
		f.diags.Unregister(config.LoopName)
		return nil
	})

	userscript, err := script.Evaluate(
		f.stop,
		f.scriptLoader,
		f.applyConfigs,
		loopDiags,
		script.TargetSchema(config.TargetSchema),
		f.watchers,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not initialize userscript for %s", config.LoopName)
	}

	// Apply logic and configurations defined by the user-script.
	if userscript.Sources.Len() > 0 || userscript.Targets.Len() > 0 {
		loop.events.fan = &scriptEvents{
			Events: loop.events.fan,
			Script: userscript,
		}
		loop.events.serial = &scriptEvents{
			Events: loop.events.serial,
			Script: userscript,
		}
	}

	loop.events.fan = (&metricsEvents{Events: loop.events.fan}).withLoopName(config.LoopName)
	loop.events.serial = (&metricsEvents{Events: loop.events.serial}).withLoopName(config.LoopName)

	loop.metrics.backfillStatus = backfillStatus.WithLabelValues(config.LoopName)

	if err := loopDiags.Register("loop", loop); err != nil {
		return nil, err
	}

	return &Loop{loop, initialPoint}, nil
}

// singletonChannel returns a channel that emits a single value and is
// closed.
func singletonChannel[T any](val T) <-chan T {
	ch := make(chan T, 1)
	ch <- val
	close(ch)
	return ch
}
