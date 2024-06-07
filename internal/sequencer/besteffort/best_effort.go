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

// Package besteffort contains a best-effort implementation of [types.MultiAcceptor].
package besteffort

import (
	"context"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/decorators"
	"github.com/cockroachdb/replicator/internal/sequencer/scheduler"
	"github.com/cockroachdb/replicator/internal/sequencer/sequtil"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
)

// BestEffort injects an acceptor shim that will attempt to write
// directly to the target table. It will also run a concurrent
// sequencer for each table that is provided.
type BestEffort struct {
	cfg         *sequencer.Config
	leases      types.Leases
	marker      *decorators.Marker
	once        *decorators.Once
	scheduler   *scheduler.Scheduler
	stagingPool *types.StagingPool
	stagers     types.Stagers
	targetPool  *types.TargetPool
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

// Start implements [sequencer.Starter]. It will launch a background
// goroutine to attempt to apply staged mutations for each table within
// the group.
func (s *bestEffort) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	stats := notify.VarOf(sequencer.NewStat(opts.Group, &ident.TableMap[hlc.Range]{}))

	sequtil.LeaseGroup(ctx, s.leases, opts.Group, func(ctx *stopper.Context, group *types.TableGroup) {
		for _, table := range opts.Group.Tables {
			table := table // Capture.

			// Create an options configuration that executes the
			// delegate Sequencer as though it were configured only as a
			// single table. The group name is changed to ensure that
			// each Start call can acquire its own lease.
			subOpts := opts.Copy()
			subOpts.MaxDeferred = s.cfg.TimestampLimit
			subOpts.Group.Name = ident.New(subOpts.Group.Name.Raw() + ":" + table.Raw())
			subOpts.Group.Tables = []ident.Table{table}

			_, subStats, err := s.delegate.Start(ctx, subOpts)
			if err != nil {
				log.WithError(err).Warnf(
					"BestEffort.Start: could not start nested Sequencer for %s", table)
				return
			}

			// Start a helper to aggregate the progress values together.
			ctx.Go(func(ctx *stopper.Context) error {
				// Ignoring error since innermost callback returns nil.
				_, _ = stopvar.DoWhenChanged(ctx, nil, subStats, func(ctx *stopper.Context, _, subStat sequencer.Stat) error {
					_, _, err := stats.Update(func(old sequencer.Stat) (sequencer.Stat, error) {
						nextProgress, ok := subStat.Progress().Get(table)
						if !ok {
							return nil, notify.ErrNoUpdate
						}
						next := old.Copy()
						next.Progress().Put(table, nextProgress)
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

		// Wait until shutdown.
		<-ctx.Stopping()
	})

	acc := opts.Delegate
	// Write staging entries only for immediately-applied mutations.
	if !s.cfg.IdempotentSource {
		acc = s.marker.MultiAcceptor(acc)
	}
	// Respect table-dependency ordering.
	acc = types.OrderedAcceptorFrom(&acceptor{s.BestEffort, acc}, s.watchers)
	// Filter repeated messages.
	if !s.cfg.IdempotentSource {
		acc = s.once.MultiAcceptor(acc)
	}
	return acc, stats, nil
}
