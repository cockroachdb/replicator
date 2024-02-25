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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/sequtil"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sijms/go-ora/v2/network"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// BestEffort looks for deferred mutations and attempts to apply them on a
// best-effort basis. Mutations to apply are marked with a lease to
// ensure an at-least-once behavior.
type BestEffort struct {
	cfg         *sequencer.Config
	leases      types.Leases
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
	failedCount := 0

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
		var pending []types.Mutation
		q, hasMore, err = s.stagers.Unstage(ctx, s.stagingPool, q,
			func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
				pending = append(pending, mut)
				return nil
			})
		if err != nil {
			return err
		}

		stat := &Stat{}
		stat.Attempted += len(pending)

		// Just as in the acceptor, we want to try applying each
		// mutation individually. We know that any mutation we're
		// operating on has been deferred at least once, so using larger
		// batches is unlikely to yield any real improvement here.
		// We can, at least, apply the mutations in parallel.
		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(s.cfg.Parallelism)

		// These accumulate the timestamps of any failed mutations.
		// They're accessed from within the goroutines launched below.
		// We use an atomic counter to allow non-conflicting writes into
		// the slice of failed timestamps.
		failedTimes := make([]hlc.Time, len(pending))
		var failedIdx atomic.Int32

		// This is a filter-in-place operation to retain only those
		// mutations that were successfully applied and which we can
		// subsequently mark as applied. Even though multiple goroutines
		// are rewriting the slice, they never access the same index,
		// and they're always behind the index of the initiating loop.
		var successIdx atomic.Int32
		for _, mut := range pending {
			mut := mut // Capture.
			eg.Go(func() error {
				batch := &types.TableBatch{
					Data:  []types.Mutation{mut},
					Table: tbl,
					Time:  mut.Time,
				}
				err := acceptor.AcceptTableBatch(egCtx, batch, &types.AcceptOptions{})
				// Save applied mutations to mark later.
				if err == nil {
					idx := successIdx.Add(1) - 1
					pending[idx] = mut
					return nil
				}

				// Record failed timestamps.
				idx := failedIdx.Add(1) - 1
				failedTimes[idx] = mut.Time

				// Ignore expected errors, we'll try again later. We'll
				// leave the lease intact so that we might skip the
				// mutation until a later cycle.
				if isNormalError(err) {
					deferrals.Inc()
					return nil
				}
				// Log any other errors, they're not going to block us
				// and we can retry later. We'll leave the lease
				// expiration intact so that we can skip this row for a
				// period of time.
				log.WithError(err).Warnf(
					"sweep: table %s; key %s; will retry mutation later",
					tbl, string(mut.Key))
				errCount.Inc()
				return nil
			})
		}

		// Wait for all apply tasks to complete. The only expected error
		// here would be context cancellation; the worker always returns
		// a nil error.
		if err := eg.Wait(); err != nil {
			return err
		}

		// Mark successful mutations and keep reading.
		if idx := successIdx.Load(); idx > 0 {
			if err := marker.MarkApplied(ctx, s.stagingPool, pending[:idx]); err != nil {
				return err
			}
			stat.Applied += int(idx)
		}

		// Locate earliest failed timestamp.
		failedTimes = failedTimes[:failedIdx.Load()]
		for _, failed := range failedTimes {
			if hlc.Compare(failed, earliestFailed) < 0 {
				earliestFailed = failed
			}
		}

		if earliestFailed.Nanos() != math.MaxInt64 {
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
		} else if earliestFailed.Nanos() == math.MaxInt64 {
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
		failedCount += len(failedTimes)
		if failedCount >= s.cfg.SweepLimit {
			sweepAbandoned.Inc()
			log.Infof("BestEffort.sweepOnce: encountered %d failed mutations in %s; backing off",
				failedCount, tbl)
			break
		}
	}
	return nil
}

// isNormalError returns true if the error represents an error that's
// likely to go away on its own in the future (e.g. FK constraints).
// These are errors that we're ok with reducing log levels for.
//
// https://github.com/cockroachdb/cdc-sink/issues/688
func isNormalError(err error) bool {
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
		return pgErr.Code == "23503" // foreign_key_violation
	} else if myErr := (*mysql.MySQLError)(nil); errors.As(err, &myErr) {
		// Cannot add or update a child row: a foreign key constraint fails
		return myErr.Number == 1452
	} else if oraErr := (*network.OracleError)(nil); errors.As(err, &oraErr) {
		switch oraErr.ErrCode {
		case 1: // ORA-0001 unique constraint violated
			// The MERGE that we execute uses read-committed reads, so
			// it's possible for two concurrent merges to attempt to
			// insert the same row.
			return true
		case 60: // ORA-00060: Deadlock detected
		// Our attempt to insert ran into another transaction, possibly
		// from a different cdc-sink instance. This can happen since a
		// MERGE operation reads before it starts writing and the order
		// in which locks are acquired may vary.
		case 2291: // ORA-02291: integrity constraint
			return true
		}
	}
	return false
}
