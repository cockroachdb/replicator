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

// Package besteffort relaxes the consistency of a target schema.
package besteffort

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/scheduler"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
)

// BestEffort relaxes the overall consistency of a target schema to
// improve throughput for smaller groups of tables defined by
// foreign-key relationships.
type BestEffort struct {
	cfg         *sequencer.Config
	scheduler   *scheduler.Scheduler
	stagers     types.Stagers
	stagingPool *types.StagingPool
	timeSource  func() hlc.Time
	watchers    types.Watchers
}

var _ sequencer.Shim = (*BestEffort)(nil)

// SetTimeSource is called by tests that need to ensure lock-step
// behaviors in sweepTable or when testing the proactive timestamp
// behavior.
func (s *BestEffort) SetTimeSource(source func() hlc.Time) {
	s.timeSource = source
}

// Wrap implements [sequencer.Shim].
func (s *BestEffort) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &bestEffort{
		BestEffort: s,
		delegate:   delegate,
	}, nil
}

type bestEffort struct {
	*BestEffort

	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*bestEffort)(nil)

// Start implements [sequencer.Starter]. It will start multiple
// instances of the delegate sequencer, once for each
// referentially-connected group of tables.
func (s *bestEffort) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	watcher, err := s.watchers.Get(opts.Group.Enclosing)
	if err != nil {
		return nil, nil, err
	}

	// Ensure the initial map has all tables in it. This ensures that
	// all tables must make some progress before the stat will advance.
	statMap := &ident.TableMap[hlc.Range]{}
	for _, table := range opts.Group.Tables {
		statMap.Put(table, hlc.RangeEmpty())
	}
	stats := notify.VarOf(sequencer.NewStat(opts.Group, statMap))

	// This will route table data to the appropriate sub-sequencer.
	ret := &router{}

	// Start a delegate sequencer for each non-overlapping subgroup of
	// tables in the target schema. This ensures that tables with FK
	// relationships can be swept in a coordinated fashion.
	for _, comp := range watcher.Get().Components {
		// Make a shallow copy and then filter by input groups.
		tables := slices.Clone(comp.Order)
		slices.DeleteFunc(tables, func(toDelete ident.Table) bool {
			return !slices.ContainsFunc(opts.Group.Tables, func(requested ident.Table) bool {
				return ident.Equal(toDelete, requested)
			})
		})

		subOpts := opts.Copy()
		subOpts.Group.Tables = tables
		subOpts.MaxDeferred = s.cfg.TimestampLimit

		subAcc, subStats, err := s.delegate.Start(ctx, subOpts)
		if err != nil {
			log.WithError(err).Warnf(
				"BestEffort.Start: could not start nested Sequencer for %s", tables)
			return nil, nil, err
		}

		// Route incoming mutations to the component's sequencer.
		for _, table := range comp.Order {
			// This is a special case for single-table groups, where
			// we'll try to write directly to the target table, rather
			// than wait for a stage-apply cycle.
			if len(comp.Order) == 1 {
				log.Tracef("enabling direct path for %s", comp.Order[0])
				subAcc = types.UnorderedAcceptorFrom(&directAcceptor{
					BestEffort: s.BestEffort,
					apply:      subOpts.Delegate,
					fallback:   subAcc,
				})
			}
			ret.routes.Put(table, subAcc)
		}

		// Start a helper to aggregate the progress values together.
		ctx.Go(func(ctx *stopper.Context) error {
			// Ignoring error since innermost callback returns nil.
			_, _ = stopvar.DoWhenChanged(ctx, nil, subStats, func(ctx *stopper.Context, _, subStat sequencer.Stat) error {
				_, _, err := stats.Update(func(old sequencer.Stat) (sequencer.Stat, error) {
					next := old.Copy()
					subStat.Progress().CopyInto(next.Progress())
					buf, _ := next.Progress().MarshalJSON()
					log.Tracef("aggregated progress: %s", buf)
					return next, nil
				})
				return err
			})
			return nil
		})
	}

	// Generate a synthetic maximum checkpoint bound in the absence
	// of any existing checkpoints. This allows partial progress to
	// be made in advance of receiving any checkpoints from the
	// source.
	ctx.Go(func(ctx *stopper.Context) error {
		for {
			if _, _, err := opts.Bounds.Update(func(old hlc.Range) (hlc.Range, error) {
				// Cancel this task once there are checkpoints.
				if old.Min() != hlc.Zero() {
					return hlc.Range{}, context.Canceled
				}
				// This source has a negative offset from the
				// current time. If there's a single, unapplied
				// checkpoint, it should be in the relative future
				// from the synthetic ones.
				proposed := s.timeSource()
				if hlc.Compare(proposed, old.MaxInclusive()) > 0 {
					return hlc.RangeIncluding(old.Min(), proposed), nil
				}
				return hlc.Range{}, notify.ErrNoUpdate
			}); err != nil {
				// Will be context.Canceled from callback above.
				return nil
			}
			select {
			case <-time.After(time.Second):
			case <-ctx.Stopping():
				return nil
			}
		}
	})

	return types.UnorderedAcceptorFrom(ret), stats, nil
}
