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
	"math"
	"sync"
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
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// BestEffort looks for deferred mutations and attempts to apply them on a
// best-effort basis. Mutations to apply are marked with a lease to
// ensure an at-least-once behavior.
type BestEffort struct {
	cfg         *sequencer.Config
	leases      types.Leases
	scheduler   *scheduler.Scheduler
	stagingPool *types.StagingPool
	stagers     types.Stagers
	targetPool  *types.TargetPool
	timeSource  func() hlc.Time
	watchers    types.Watchers
}

var _ sequencer.Sequencer = (*BestEffort)(nil)

// SetTimeSource is called by tests that need to ensure lock-step
// behaviors in sweepTable or when testing the proactive timestamp
// behavior.
func (s *BestEffort) SetTimeSource(source func() hlc.Time) {
	s.timeSource = source
}

// Start implements [sequencer.Starter]. It will launch a background
// goroutine to attempt to apply staged mutations for each table within
// the group.
func (s *BestEffort) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	stats := &notify.Var[sequencer.Stat]{}
	stats.Set(newStat(opts.Group))

	delegate := opts.Delegate

	// Identify which table's we're working on.
	gauges := make([]prometheus.Gauge, len(opts.Group.Tables))
	for idx, tbl := range opts.Group.Tables {
		gauges[idx] = sweepActive.WithLabelValues(metrics.TableValues(tbl)...)
	}

	// Acquire a lease on the group name to prevent multiple sweepers
	// from operating. In the future, this lease could be moved down
	// into the per-table method to allow multiple cdc-sink instances to
	// process different tables.
	//
	// https://github.com/cockroachdb/cdc-sink/issues/687
	sequtil.LeaseGroup(ctx, s.leases, opts.Group, func(ctx *stopper.Context, group *types.TableGroup) {
		// Launch goroutine(s) to handle sweeping for each table in the group.
		for idx, table := range group.Tables {
			gauges[idx].Set(1)
			s.sweepTable(ctx, table, opts.Bounds, stats, delegate)
		}
		// We want to hold the lease until we're shut down.
		<-ctx.Stopping()
		for _, gauge := range gauges {
			gauge.Set(0)
		}
	})

	// Respect table-dependency ordering.
	acc := types.OrderedAcceptorFrom(&acceptor{s, delegate}, s.watchers)

	return acc, stats, nil
}

// sweepTable implements the per-table loop behavior and communicates
// progress via the stats argument. It is non-blocking and will start
// background goroutines to process the table.
func (s *BestEffort) sweepTable(
	ctx *stopper.Context,
	table ident.Table,
	bounds *notify.Var[hlc.Range],
	stats *notify.Var[sequencer.Stat],
	acceptor types.TableAcceptor,
) {
	// The BestEffort sweeper may have an extremely large amount of work
	// to perform when operating on an initial backfill that contains FK
	// references. We want to ensure that the call to sweepOnce is able
	// to communicate partial progress on a regular basis, since the
	// overall sweep process may take an unbounded amount of time.
	partialProgress := &notify.Var[*Stat]{}
	ctx.Go(func() error {
		_, err := stopvar.DoWhenChanged(ctx, nil, partialProgress, func(ctx *stopper.Context, _, partial *Stat) error {
			_, _, err := stats.Update(func(old sequencer.Stat) (sequencer.Stat, error) {
				nextImpl := old.(*Stat).copy()
				// We want to export counters and last-state for testing.
				nextImpl.Attempted += partial.Attempted
				nextImpl.Applied += partial.Applied
				nextImpl.Progress().Put(table, partial.LastProgress)
				return nextImpl, nil
			})
			return err
		})
		return err
	})

	// This goroutine will sweep for mutations in the staging table.
	ctx.Go(func() error {
		log.Tracef("BestEffort starting on %s", table)

		_, err := stopvar.DoWhenChangedOrInterval(ctx,
			hlc.RangeEmpty(), bounds, s.cfg.QuiescentPeriod,
			func(ctx *stopper.Context, _, bound hlc.Range) error {
				// The bounds will be empty in the idle on initial-backfill
				// condition. We still want to be able to proactively sweep
				// for mutations, so we'll invent a fake bound timestamp
				// that won't be reported. This allows us to continue to
				// make partial progress on staged mutations within an
				// initial-backfill.
				report := partialProgress
				if bound.Empty() {
					bound = hlc.RangeIncluding(bound.Min(), s.timeSource())
					if bound.Empty() {
						return nil
					}
					report = nil
				}
				if err := s.sweepOnce(ctx, table, bound, acceptor, report); err != nil {
					// We'll sleep and then retry with the same or similar bounds.
					log.WithError(err).Warnf(
						"BestEffort: error while sweeping table %s; will continue in %s",
						table, s.cfg.QuiescentPeriod)
					return nil
				}
				return nil
			})
		log.Tracef("BestEffort stopping on %s", table)
		return err
	})
}

// Stat is emitted by [BestEffort]. This is used by test code to inspect
// the (partial) progress that may be made.
type Stat struct {
	sequencer.Stat

	Applied      int      // The number of mutations that were actually applied.
	Attempted    int      // The number of mutations that were seen.
	LastProgress hlc.Time // The time that the table arrived at.
}

// newStat returns an initialized Stat.
func newStat(group *types.TableGroup) *Stat {
	return &Stat{
		Stat: sequencer.NewStat(group, &ident.TableMap[hlc.Time]{}),
	}
}

// Copy implements [sequencer.Stat].
func (s *Stat) Copy() sequencer.Stat { return s.copy() }

// copy creates a deep copy of the Stat.
func (s *Stat) copy() *Stat {
	next := *s
	next.Stat = next.Stat.Copy()
	return &next
}

// sweepOnce will execute a single pass for deferred, un-leased
// mutations within the time range. The returned Stat will be nil if the
// context is stopped.
func (s *BestEffort) sweepOnce(
	ctx *stopper.Context,
	tbl ident.Table,
	bounds hlc.Range,
	acceptor types.TableAcceptor,
	partialProgress *notify.Var[*Stat],
) error {
	start := time.Now()
	tblValues := metrics.TableValues(tbl)
	deferrals := sweepDeferrals.WithLabelValues(tblValues...)
	duration := sweepDuration.WithLabelValues(tblValues...)
	errCount := sweepErrors.WithLabelValues(tblValues...)
	sweepAbandoned := sweepAbandonedCount.WithLabelValues(tblValues...)
	sweepAttempted := sweepAttemptedCount.WithLabelValues(tblValues...)
	sweepApplied := sweepAppliedCount.WithLabelValues(tblValues...)
	sweepLastAttempt.WithLabelValues(tblValues...).SetToCurrentTime()

	// We expect to see an unbounded number of mutations that can't be
	// applied. We want to record the earliest time of the failed
	// mutation and the total number of failed mutations that we've
	// seen. When we exceed a threshold of failed mutations we'll give
	// up in this iteration. The assumption here is that failed
	// mutations will succeed after some other tables make progress. By
	// choosing to restart, we'll retry those oldest mutations sooner.
	// Being able to move our starting scan time forward reduces the
	// number of applied mutations that must be filtered by the unstage
	// query. Ideally, the workers started in this method will be in a
	// position to "run behind" the worker(s) for the parent tables.
	earliestFailed := hlc.New(math.MaxInt64, math.MaxInt)

	// We need to ensure that updates to any give key remain in a
	// time-ordered fashion. If we fail a key at T1 it needs to stay
	// failed at any subsequent time that we may encounter it.
	poisonedKeys := make(map[string]struct{})

	log.Tracef("BestEffort.sweepOnce: starting %s in %s", tbl, bounds)
	marker, err := s.stagers.Get(ctx, tbl)
	if err != nil {
		return err
	}
	q := &types.UnstageCursor{
		// This is a hack until Unstage can tell us how many deferred
		// mutations were skipped.
		//
		// TODO: We need a signal from Unstage to indicate that there
		// are lease-filtered mutations so we know not to advance the
		// minimum bound on the resolving window.
		IgnoreLeases:   true,
		StartAt:        bounds.Min(),
		EndBefore:      bounds.Max(),
		Targets:        []ident.Table{tbl},
		TimestampLimit: s.cfg.TimestampLimit,
		UpdateLimit:    s.cfg.SweepLimit,
	}
	for hasMore := true; hasMore && !ctx.IsStopping(); {
		// Set a lease to prevent the mutations from being marked as
		// applied.
		q.LeaseExpiry = time.Now().Add(s.cfg.QuiescentPeriod)

		// Collect some number of mutations.
		pending := &types.MultiBatch{}
		q, hasMore, err = s.stagers.Unstage(ctx, s.stagingPool, q,
			func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
				return pending.Accumulate(tbl, mut)
			})
		if err != nil {
			return err
		}
		pendingCount := pending.Count()

		stat := &Stat{}
		stat.Attempted += pendingCount

		// Split the large batch of data into reasonably-sized segments
		// that can be attempted in parallel. Even though an entire
		// segment is accepted or rejected atomically, and it would be
		// ideal to attempt each mutation individually, there's a ~5ms
		// minimum latency for any given database transaction. This
		// limits our total throughput, even at high levels of SQL
		// concurrency. Instead, we'll take a pragmatic middle ground
		// to optimize for bulk backfill or catch-up cases.
		//
		// A guarantee provided by this segmentation code is that all
		// mutations for a given key will be in the same segment. This
		// avoids having to perform error-tracking across
		// concurrently-executing segments.
		segments := sequtil.SegmentTableBatches(pending, tbl, s.cfg.FlushSize)

		// Synchronize external accesses from callback below.
		var resultMu sync.RWMutex

		// This accumulates the mutations from segments that were
		// successfully applied and which we can subsequently mark as
		// applied.
		successMuts := make([]types.Mutation, 0, pendingCount)

		outcomes := make([]*notify.Var[*lockset.Status], len(segments))
		for idx, segment := range segments {
			segment := segment // Capture.
			outcomes[idx] = s.scheduler.TableBatch(segment, func() error {
				// Filter any keys poisoned by previous tasks.
				resultMu.RLock()
				if len(poisonedKeys) > 0 {
					idx := 0
					for _, mut := range segment.Data {
						if _, poisoned := poisonedKeys[string(mut.Key)]; !poisoned {
							segment.Data[idx] = mut
							idx++
						}
					}
					segment.Data = segment.Data[:idx]
				}
				resultMu.RUnlock()

				// Perform database access outside critical section.
				err := acceptor.AcceptTableBatch(ctx, segment, &types.AcceptOptions{})

				resultMu.Lock()
				defer resultMu.Unlock()

				// Save applied mutations to mark later.
				if err == nil {
					successMuts = append(successMuts, segment.Data...)
					return nil
				}

				// Record failed keys and minimum timestamps.
				for _, mut := range segment.Data {
					poisonedKeys[string(mut.Key)] = struct{}{}
					if hlc.Compare(mut.Time, earliestFailed) < 0 {
						earliestFailed = mut.Time
					}
				}

				// Ignore expected errors, we'll try again later.
				if sequtil.IsDeferrableError(err) {
					deferrals.Inc()
					return nil
				}

				// Log any other errors, they're not going to block us
				// and we can retry later. We'll leave the lease
				// expiration intact so that we can skip this row for a
				// period of time.
				log.WithError(err).Warnf(
					"sweep: table %s; will retry %d mutations later",
					tbl, segment.Count())
				errCount.Inc()
				return nil
			})
		}

		// Wait for all apply tasks to complete. The only expected error
		// here would be context cancellation; the worker always returns
		// a nil error.
		if err := lockset.Wait(ctx, outcomes); err != nil {
			return err
		}

		// Mark successful mutations and keep reading.
		if len(successMuts) > 0 {
			if err := marker.MarkApplied(ctx, s.stagingPool, successMuts); err != nil {
				return err
			}
			stat.Applied += len(successMuts)
		}

		if len(poisonedKeys) > 0 {
			// We failed a mutation, so we can't advance beyond the time
			// of that mutation. Reporting this time will allow us to
			// retry the failed mutation later.
			stat.LastProgress = earliestFailed.Before()
		} else if hasMore {
			// We did not see any failed mutations within this
			// iteration, but there may be additional mutations to
			// unstage later. We can report that all times before the
			// minimum offset within the cursor have been successfully
			// applied.
			stat.LastProgress = q.MinOffset().Before()
		} else {
			// There are no more mutations that can be unstaged within
			// the requested bounds. Since we've completed all possible
			// work to perform, we can advance our progress report to
			// the last timestamp within the requested range (max is
			// exclusive). In this case, we can report the end time used
			// in the sweep
			stat.LastProgress = bounds.MaxInclusive()
		}

		// Report progress to observer so that we may mark checkpoints
		// as having been completed. We won't report partial progress if
		// we've invented a fake timestamp to use during an initial
		// backfill. This ensures that we can handle data arriving into
		// the staging tables in arbitrary order; we keep reading from
		// timestamp zero until the first checkpoint timestamp has been
		// received and fully processed.
		if partialProgress != nil {
			partialProgress.Set(stat)
		}

		log.Tracef("BestEffort.sweepOnce: completed %s (%d applied of %d attempted) to %s",
			tbl, stat.Applied, stat.Attempted, stat.LastProgress)
		duration.Observe(time.Since(start).Seconds())
		sweepAttempted.Add(float64(stat.Attempted))
		sweepApplied.Add(float64(stat.Applied))

		// We may have failed out too many mutations, so we'll exit
		// early in the hopes that a short delay will allow this table
		// to run behind its ancestor table(s).
		if len(poisonedKeys) >= s.cfg.SweepLimit {
			sweepAbandoned.Inc()
			log.Infof("BestEffort.sweepOnce: encountered %d failed mutations in %s; backing off",
				len(poisonedKeys), tbl)
			break
		}
	}
	return nil
}
