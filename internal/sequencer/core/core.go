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

// Package core contains a sequencer implementation that preserves
// source transactions and relative timestamps.
package core

import (
	"context"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/scheduler"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/sequtil"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/lockset"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// The Core sequencer accepts batches by writing them to a staging
// table. It will then apply the data in a transactionally-consistent
// and possibly concurrent fashion.
type Core struct {
	cfg         *sequencer.Config
	leases      types.Leases
	scheduler   *scheduler.Scheduler
	stagers     types.Stagers
	stagingPool *types.StagingPool
	targetPool  *types.TargetPool
}

var _ sequencer.Sequencer = (*Core)(nil)

// Start implements [sequencer.Sequencer]. It will incrementally unstage
// and apply batches of mutations within the given bounds.
func (s *Core) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	progress := notify.VarOf(sequencer.NewStat(opts.Group, &ident.TableMap[hlc.Range]{}))

	// Acquire a lease on the group name to prevent multiple sweepers
	// from operating.
	sequtil.LeaseGroup(ctx, s.leases, opts.Group, func(ctx *stopper.Context, group *types.TableGroup) {
		// Report which instance of cdc-sink is processing the tables
		// within the group.
		activeGauges := make([]prometheus.Gauge, len(group.Tables))
		for idx, tbl := range group.Tables {
			activeGauges[idx] = sweepActive.WithLabelValues(metrics.TableValues(tbl)...)
		}
		for _, active := range activeGauges {
			active.Set(1)
		}
		defer func() {
			for _, active := range activeGauges {
				active.Set(0)
			}
		}()

		// Allow a soft failure mode in best-effort mode.
		poisoned := newPoisonSet(opts.MaxDeferred)

		// Start a process to report ongoing progress. We receive data
		// to apply in an ordered fashion below, but will ultimately
		// apply them in a concurrent fashion. This is a channel of
		// channels that will each emit a single timestamp when the
		// associated apply task has finished successfully. It ensures
		// that timestamp updates are received in an ordered fashion.
		progressReports := make(chan (<-chan hlc.Range), s.cfg.Parallelism+1)
		ctx.Go(func() error {
			for {
				select {
				case <-ctx.Stopping():
					return nil
				case progressReport := <-progressReports:
					select {
					case advanceTo, advanced := <-progressReport:
						// Discard progress reports if there are
						// poisoned keys that will need to be retried.
						if !poisoned.IsClean() {
							continue
						}
						// This should not happen. We should only
						// receive a closed channel if there were
						// poisoned records already.
						if !advanced {
							poisoned.ForceFull()
							continue
						}
						if log.IsLevelEnabled(log.TraceLevel) {
							log.Tracef("progress update: %s to %s (lag %s)",
								group, advanceTo,
								time.Since(time.Unix(0, advanceTo.MaxInclusive().Nanos())))
						}
						_, _, _ = progress.Update(func(stat sequencer.Stat) (sequencer.Stat, error) {
							stat = stat.Copy()
							for _, table := range group.Tables {
								stat.Progress().Put(table, advanceTo)
							}
							return stat, nil
						})
					case <-ctx.Stopping():
						return nil
					}
				}
			}
		})

		// Open a reader over the staging tables. It will respond to
		// updates in the timestamp bounds and provide a sequence of
		// event notifications that are interpreted by a Copier below.
		stagingReader, err := s.stagers.Read(ctx, &types.StagingQuery{
			Bounds:       opts.Bounds,
			FragmentSize: s.cfg.ScanSize,
			Group:        group,
		})
		if err != nil {
			log.WithError(err).Warnf(
				"could not open staging table reader for %s; will retry",
				group)
		}

		// We'll set up a template instance of round and then stamp out
		// copies as needed. This avoids the overhead of re-creating the
		// metrics collectors.
		metricLabels := metrics.SchemaValues(group.Enclosing)
		template := &round{
			Core:     s,
			delegate: opts.Delegate,
			group:    group,
			poisoned: poisoned,

			// Metrics.
			applied:     sweepAppliedCount.WithLabelValues(metricLabels...),
			duration:    sweepDuration.WithLabelValues(metricLabels...),
			lastAttempt: sweepLastAttempt.WithLabelValues(metricLabels...),
			lastSuccess: sweepLastSuccess.WithLabelValues(metricLabels...),
			skew:        sweepSkewCount.WithLabelValues(metricLabels...),
		}
		nextRound := func() *round {
			cpy := *template
			return &cpy
		}

		// This may receive multiple flush callbacks if a large batch
		// has been segmented.
		var accumulator *round
		// Async error reporting from tasks.
		errorReports := make(chan lockset.Outcome, s.cfg.Parallelism+1)
		flushAccumulator := func() {
			// Create a channel to report checkpoint progress and
			// enqueue it to maintain correct time order.
			report := make(chan hlc.Range, 1)
			select {
			case progressReports <- report:
			case <-ctx.Stopping():
				return
			}

			// Start a task to store the new data. Hand the outcome
			// object off to the top-level monitoring loop.
			commitOutcome := accumulator.scheduleCommit(ctx, report)
			accumulator = nil
			select {
			case errorReports <- commitOutcome:
			case <-ctx.Stopping():
			}
		}

		// A Copier aggregates the individual temporal batches of data
		// produced by the staging reader into two callbacks, Progress and
		// Flush. The Progress callback happens when the reader has caught
		// up to the end of the checkpoint bounds; it allows us to
		// report progress up to the end of the checkpoint range (i.e. a
		// changefeed resolved timestamp). The Flush callback is where
		// we launch, possibly concurrent, tasks to apply data to the
		// target.
		copier := &sequtil.Copier{
			Config: s.cfg,
			Source: stagingReader,
			// We'll get this callback when the copier does not expect
			// any more data to arrive until the bounds are updated.
			Progress: func(ctx *stopper.Context, progress hlc.Range) error {
				log.Tracef("progress notification for %s: %s", group, progress)

				// The last flush was a fragment, but it turned out that
				// there actually weren't any more rows to consume when
				// the db query was retried.  We'll go ahead and flush
				// the accumulator.
				if accumulator != nil {
					flushAccumulator()
				}

				// Report the maximum timestamp. This report will be
				// ordered after any partial progress updates that are
				// made by the tasks started in the Flush callback.
				report := make(chan hlc.Range, 1)
				report <- progress
				close(report)

				select {
				case progressReports <- report:
				case <-ctx.Stopping():
				}
				return nil
			},
			Flush: func(ctx *stopper.Context, batch *types.MultiBatch, fragment bool) error {
				// Short-circuit if we've hit too many bad keys.
				if poisoned.IsFull() {
					return errPoisoned
				}

				// Fail forward if we can't apply the batch. This call
				// will also implicitly contaminate all keys in the
				// batch.
				if poisoned.IsPoisoned(batch) {
					accumulator = nil
					return nil
				}

				// Initialize if not carrying over from a previous
				// segmented read.
				if accumulator == nil {
					accumulator = nextRound()
				}

				if err := accumulator.accumulate(batch); err != nil {
					accumulator = nil
					return err
				}

				// If it's a partial fragment, we'll keep the accumulator
				// into the next update.
				if fragment {
					return nil
				}

				flushAccumulator()
				return nil
			},
		}
		if log.IsLevelEnabled(log.TraceLevel) {
			copier.Each = func(ctx *stopper.Context, batch *types.TemporalBatch, segment bool) error {
				log.Tracef("scanned batch for %s, count=%d, time=%s", group, batch.Count(), batch.Time)
				return nil
			}
		}

		// Adapt copier.Run() as if it were another lockset task.
		copyOutcome := lockset.NewOutcome()
		ctx.Go(func() error {
			err := copier.Run(ctx)
			copyOutcome.Set(lockset.StatusFor(err))
			return nil
		})

		// Monitor background tasks for failure.
		toMonitor := []lockset.Outcome{copyOutcome}
		for {
			// Filter successes or exit on errors.
			idx := 0
			for _, outcome := range toMonitor {
				status, _ := outcome.Get()
				if status.Success() {
					continue
				} else if err := status.Err(); err != nil {
					if errors.Is(err, errPoisoned) {
						// This is a non-error in best-effort mode.
						log.Tracef("reach soft FK error limit (%d) for %s; backing off",
							opts.MaxDeferred, group)
					} else if !errors.Is(err, context.Canceled) {
						log.WithError(err).Warnf("error while copying data for %s; will restart", group)
					}
					return
				} else {
					toMonitor[idx] = outcome
					idx++
				}
			}
			toMonitor = toMonitor[:idx]

			select {
			case <-time.After(time.Second):
				// Perform maintenance
			case taskOutcome := <-errorReports:
				// New task launched, monitor it.
				toMonitor = append(toMonitor, taskOutcome)
			case <-ctx.Stopping():
				// Clean shutdown
				return
			}
		}
	})
	return &acceptor{s}, progress, nil
}
