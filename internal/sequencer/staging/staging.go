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

// Package staging contains a [sequencer.Shim] that writes incoming
// mutations to staging.
package staging

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/decorators"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Staging is a [sequencer.Shim] that returns an acceptor that writes
// incoming data to staging tables.
type Staging struct {
	cfg         *sequencer.Config
	markers     *decorators.Marker
	stagers     types.Stagers
	stagingPool *types.StagingPool
	watchers    types.Watchers
}

var _ sequencer.Shim = (*Staging)(nil)

// Wrap implements [sequencer.Shim].
func (s *Staging) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &staging{s, delegate}, nil
}

type staging struct {
	*Staging
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*staging)(nil)

// Start injects a staging-table query as an asynchronous data source to
// be passed into the next (core) sequencer.
func (s *staging) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (*notify.Var[sequencer.Stat], error) {
	// Prepare to read from the staging tables.
	reader, err := s.stagers.Query(ctx, &types.StagingQuery{
		Bounds:       opts.Bounds,
		FragmentSize: s.cfg.ScanSize,
		Group:        opts.Group,
	})
	if err != nil {
		return nil, err
	}

	// Start a process to consume and stage incoming mutations.
	if err := s.consume(ctx, opts.BatchReader, opts.Terminal); err != nil {
		return nil, err
	}

	opts = opts.Copy()
	// Supply staging data downstream.
	opts.BatchReader = reader
	// Mark successful batches.
	opts.Delegate = s.markers.MultiAcceptor(opts.Delegate)
	// Staging represents a terminal state.
	opts.Terminal = nil

	return s.delegate.Start(ctx, opts)
}

func (s *staging) consume(ctx *stopper.Context, source types.BatchReader, terminal sequencer.TerminalFunc) error {
	if source == nil {
		return errors.New("no BatchReader provided")
	}
	ctx.Go(func(ctx *stopper.Context) error {
		for !ctx.IsStopping() {
			if err := s.consumeOnce(ctx, source, terminal); err != nil {
				log.WithError(err).Warn("error while staging data; will restart")
			}
		}
		return nil
	})
	return nil
}

func (s *staging) consumeOnce(ctx *stopper.Context, source types.BatchReader, terminal sequencer.TerminalFunc) error {
	// Create a nested stopper for error handling.
	ctx = stopper.WithContext(ctx)

	ch, err := source.Read(ctx)
	if err != nil {
		return err
	}

	// Start parallel stagers.
	for range s.cfg.Parallelism {
		ctx.Go(func(ctx *stopper.Context) error {
			for {
				var cur *types.BatchCursor
				select {
				case cur = <-ch:
				case <-ctx.Stopping():
					return nil
				}
				// Read from closed channel during shutdown.
				if cur == nil {
					return nil
				}
				// This will cause the nested context to stop.
				if cur.Error != nil {
					return errors.Wrap(cur.Error, "input reader failed")
				}
				if err := s.stageCursor(ctx, cur, terminal); err != nil {
					return err
				}
			}
		})
	}

	return ctx.Wait()
}

func (s *staging) stageCursor(ctx *stopper.Context, cur *types.BatchCursor, terminal sequencer.TerminalFunc) (err error) {
	log.Tracef("input reader progress %s", cur.Progress)

	// Error handling varies based on whether a callback has been
	// installed. If we can report errors out-of-band, we'll let the
	// hook replace the error.
	defer func() {
		if terminal == nil || cur.Batch == nil {
			return
		}
		err = terminal(ctx, cur.Batch, err)
	}()

	// Progress-only update.
	if cur.Batch == nil {
		return nil
	}

	var lastSchema ident.Schema
	var sd *types.SchemaData
	for table := range cur.Batch.Data.Keys() {
		if !ident.Equal(lastSchema, table.Schema()) {
			watcher, err := s.watchers.Get(table.Schema())
			if err != nil {
				return err
			}
			sd = watcher.Get()
		}
		if _, ok := sd.Columns.Get(table); !ok {
			return errors.Errorf("cannot stage data for unknown table: %s", table)
		}
	}

	ct := 0
	for tableBatch := range cur.Batch.Data.Values() {
		stager, err := s.stagers.Get(ctx, tableBatch.Table)
		if err != nil {
			return err
		}

		if err := retry.Retry(ctx, s.stagingPool, func(ctx context.Context) error {
			return stager.Stage(ctx, s.stagingPool, tableBatch.Data)
		}); err != nil {
			return err
		}

		if log.IsLevelEnabled(log.TraceLevel) {
			ct += tableBatch.Count()
		}
	}
	log.Tracef("staged %d mutations", ct)
	return nil
}
