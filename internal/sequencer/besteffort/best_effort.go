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
	cfg              *sequencer.Config
	disableProactive bool
	leases           types.Leases
	stagingPool      *types.StagingPool
	stagers          types.Stagers
	targetPool       *types.TargetPool
	watchers         types.Watchers
}

var _ sequencer.Sequencer = (*BestEffort)(nil)

// DisableProactive is called by tests that need to ensure lock-step
// behaviors in sweepTable. If set to false, the sweeper will not
// synthesize fake timestamps in the initial-backfill case. This must be
// called before Start.
func (s *BestEffort) DisableProactive() {
	s.disableProactive = true
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
		// Launch a goroutine to handle sweeping for each table in the group.
		for idx, table := range group.Tables {
			idx, table := idx, table // Capture.
			ctx.Go(func() error {
				log.Tracef("BestEffort starting on %s", table)

				gauges[idx].Set(1)
				defer gauges[idx].Set(0)

				s.sweepTable(ctx, table, opts.Bounds, stats, delegate)
				log.Tracef("BestEffort stopping on %s", table)
				return nil
			})
		}
		// We want to hold the lease until we're shut down.
		<-ctx.Stopping()
	})

	// Respect table-dependency ordering.
	acc := types.OrderedAcceptorFrom(&acceptor{s, delegate}, s.watchers)

	return acc, stats, nil
}

// sweepTable implements the per-table loop behavior and communicates
// progress via the stats argument. It is blocking and should be called
// from a stoppered goroutine.
func (s *BestEffort) sweepTable(
	ctx *stopper.Context,
	table ident.Table,
	bounds *notify.Var[hlc.Range],
	stats *notify.Var[sequencer.Stat],
	acceptor types.TableAcceptor,
) {
	_, _ = stopvar.DoWhenChangedOrInterval(ctx, hlc.Range{}, bounds, s.cfg.QuiescentPeriod,
		func(ctx *stopper.Context, _, bound hlc.Range) error {
			// The bounds will be empty in the idle on initial-backfill
			// condition. We still want to be able to proactively sweep
			// for mutations, so we'll invent a fake bound timestamp
			// that won't be reported. This allows us to continue to
			// make partial progress on staged mutations within an
			// initial-backfill.
			updateProgress := true
			if bound.Empty() {
				// Set by testing to disable this behavior.
				if s.disableProactive {
					return nil
				}
				bound = hlc.Range{bound.Min(), hlc.New(time.Now().UnixNano(), 1)}
				updateProgress = false
				if bound.Empty() {
					log.Warnf("minimum bound appears to be in the future: %s", bound)
					return nil
				}
			}
			tableStat, err := s.sweepOnce(ctx, table, bound, acceptor)
			if err != nil {
				// We'll sleep and then retry with the same or similar bounds.
				log.WithError(err).Warnf("BestEffort: error while sweeping table %s; will continue", table)
				return nil
			}
			// Don't update the stat if we're shutting down, since not
			// all work will have been performed.
			if ctx.IsStopping() {
				return nil
			}
			// Ignoring error since callback returns nil.
			_, _, _ = stats.Update(func(old sequencer.Stat) (sequencer.Stat, error) {
				nextImpl := old.(*Stat).copy()
				// We want to export stats for testing.
				nextImpl.Attempted += tableStat.Attempted
				nextImpl.Applied += tableStat.Applied
				if updateProgress {
					nextImpl.Progress().Put(table, tableStat.LastTime)
				}
				return nextImpl, nil
			})
			return nil
		})
}

// Stat is emitted by [BestEffort]. This is used by test code to inspect
// the (partial) progress that may be made.
type Stat struct {
	sequencer.Stat

	Applied   int      // The number of mutations that were actually applied.
	Attempted int      // The number of mutations that were seen.
	LastTime  hlc.Time // The time that the table arrived at.
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
	ctx *stopper.Context, tbl ident.Table, bounds hlc.Range, acceptor types.TableAcceptor,
) (*Stat, error) {
	start := time.Now()
	tblValues := metrics.TableValues(tbl)
	deferrals := sweepDeferrals.WithLabelValues(tblValues...)
	duration := sweepDuration.WithLabelValues(tblValues...)
	errCount := sweepErrors.WithLabelValues(tblValues...)
	sweepAttempted := sweepAttemptedCount.WithLabelValues(tblValues...)
	sweepApplied := sweepAppliedCount.WithLabelValues(tblValues...)
	sweepLastAttempt.WithLabelValues(tblValues...).SetToCurrentTime()

	log.Tracef("BestEffort.sweepOnce: starting %s in %s", tbl, bounds)
	marker, err := s.stagers.Get(ctx, tbl)
	if err != nil {
		return nil, err
	}
	stat := &Stat{}
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
	for hasMore := true; hasMore; {
		// Exit with no result if we're shutting down.
		if ctx.IsStopping() {
			return nil, nil
		}
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
			return nil, err
		}
		stat.Attempted += len(pending)

		// Just as in the acceptor, we want to try applying each
		// mutation individually. We know that any mutation we're
		// operating on has been deferred at least once, so using larger
		// batches is unlikely to yield any real improvement here.
		// We can, at least, apply the mutations in parallel.
		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(s.cfg.Parallelism)

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
			return nil, err
		}

		// Mark successful mutations and keep reading.
		if idx := successIdx.Load(); idx > 0 {
			if err := marker.MarkApplied(ctx, s.stagingPool, pending[:idx]); err != nil {
				return nil, err
			}
			stat.Applied += int(idx)
		}
	}

	// We'll allow the resolving window to advance only if we applied
	// all mutations that we saw.
	if stat.Attempted == stat.Applied {
		stat.LastTime = bounds.Max()
	} else {
		stat.LastTime = bounds.Min()
	}

	log.Tracef("BestEffort.sweepOnce: completed %s (%d applied of %d attempted) to %s",
		tbl, stat.Applied, stat.Attempted, stat.LastTime)
	duration.Observe(time.Since(start).Seconds())
	sweepAttempted.Add(float64(stat.Attempted))
	sweepApplied.Add(float64(stat.Applied))
	return stat, nil
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
