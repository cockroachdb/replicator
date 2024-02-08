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

// Package serial contains a sequencer that preserves source
// transactions and relative timestamps.
package serial

import (
	"context"
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
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// The Serial sequencer accepts batches by writing them to a staging
// table. It will then apply the data in a transactionally-consistent
// fashion.
type Serial struct {
	cfg         *sequencer.Config
	leases      types.Leases
	stagers     types.Stagers
	stagingPool *types.StagingPool
	targetPool  *types.TargetPool
}

var _ sequencer.Sequencer = (*Serial)(nil)

// Start implements [sequencer.Sequencer]. It will incrementally unstage
// and apply batches of mutations within the given bounds.
func (s *Serial) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	stats := &notify.Var[sequencer.Stat]{}
	stats.Set(sequencer.NewStat(opts.Group, &ident.TableMap[hlc.Time]{}))
	delegate := opts.Delegate

	activeGauges := make([]prometheus.Gauge, len(opts.Group.Tables))
	for idx, tbl := range opts.Group.Tables {
		activeGauges[idx] = sweepActive.WithLabelValues(metrics.TableValues(tbl)...)
	}

	// Acquire a lease on the group name to prevent multiple sweepers
	// from operating. In the future, this lease could be moved down
	// into the per-table method to allow multiple cdc-sink instances to
	// process different tables.
	sequtil.LeaseGroup(ctx, s.leases, opts.Group, func(ctx *stopper.Context, group *types.TableGroup) {
		for _, active := range activeGauges {
			active.Set(1)
		}
		defer func() {
			for _, active := range activeGauges {
				active.Set(0)
			}
		}()

		// Ignoring error since it's always nil.
		_, _ = stopvar.DoWhenChangedOrInterval(ctx, hlc.Range{}, opts.Bounds, s.cfg.QuiescentPeriod,
			func(ctx *stopper.Context, _, bound hlc.Range) error {
				// Do nothing if, e.g. the caller has advanced the min time to the max time.
				if bound.Empty() {
					return nil
				}
				for {
					// Apply mutations to the target.
					advancedTo, moreWork, err := s.sweepOnce(ctx, group, bound, delegate)
					if err != nil {
						if !errors.Is(err, context.Canceled) {
							// We'll retry later likely with the same or similar bounds.
							log.WithError(err).Warnf("Serial: error while sweeping tables; will continue")

						}
						return nil
					}

					// Notify manager that the tables have advanced to a new
					// time. The caller can choose to make use of this
					// information, e.g. to update a resolved-timestamp table.
					_, _, _ = stats.Update(func(stat sequencer.Stat) (sequencer.Stat, error) {
						stat = stat.Copy()
						for _, table := range group.Tables {
							stat.Progress().Put(table, advancedTo)
						}
						return stat, nil
					})

					// If there's no more work, exit.
					if !moreWork {
						return nil
					}
				}
			})
	})
	return &acceptor{s}, stats, nil
}

// sweepOnce performs a single, transactionally-consistent pass over the
// staging tables for the group. It returns the timestamp that was
// processed, or bounds.Max() if no work was performed.
func (s *Serial) sweepOnce(
	ctx *stopper.Context, group *types.TableGroup, bounds hlc.Range, acceptor types.MultiAcceptor,
) (advancedTo hlc.Time, moreWork bool, _ error) {
	start := time.Now()
	log.Tracef("Serial.sweepOnce starting for %s", group)

	// Ensure that all tables we're operating on live in the same
	// schema. This limitation could be lifted if we can merge table
	// dependency ordering across schemas.
	var schema ident.Schema
	for idx, table := range group.Tables {
		if idx == 0 {
			schema = table.Schema()
			continue
		}
		if !ident.Equal(schema, table.Schema()) {
			return hlc.Zero(), false, errors.Errorf("TableGroup %s contains mixed schemas", group)
		}
	}

	labels := metrics.SchemaValues(schema)
	sweepLastAttempt.WithLabelValues(labels...).SetToCurrentTime()
	applied := sweepAppliedCount.WithLabelValues(labels...)
	duration := sweepDuration.WithLabelValues(labels...)
	skew := sweepSkewCount.WithLabelValues(labels...)
	success := sweepLastSuccess.WithLabelValues(labels...)
	unstage := sweepUnstageDuration.WithLabelValues(labels...)

	q := &types.UnstageCursor{
		StartAt:        bounds.Min(),
		EndBefore:      bounds.Max(),
		IgnoreLeases:   true, // Ignore any markers left by the best-effort sequencer.
		Targets:        group.Tables,
		TimestampLimit: s.cfg.TimestampLimit,
	}

	// Open a staging database transaction to retrieve unstaged mutations.
	stagingTx, err := s.stagingPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return hlc.Zero(), false, errors.WithStack(err)
	}
	defer func() { _ = stagingTx.Rollback(ctx) }()

	// Accumulate work in a time- and table-grouped fashion.
	work := &types.MultiBatch{}
	q, moreWork, err = s.stagers.Unstage(ctx, stagingTx, q,
		func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
			return work.Accumulate(tbl, mut)
		})
	if err != nil {
		return hlc.Zero(), false, err
	}
	unstage.Observe(time.Since(start).Seconds())

	workCount := work.Count()
	// We're done, just exit.
	if workCount == 0 && !moreWork {
		return q.EndBefore, false, nil
	}

	targetTx, err := s.targetPool.BeginTx(ctx, nil)
	if err != nil {
		return hlc.Zero(), false, errors.WithStack(err)
	}
	defer func() { _ = targetTx.Rollback() }()

	// Provide downstream acceptors with access to our transactions.
	opts := &types.AcceptOptions{
		StagingQuerier: stagingTx,
		TargetQuerier:  targetTx,
	}
	if err := acceptor.AcceptMultiBatch(ctx, work, opts); err != nil {
		return hlc.Zero(), false, err
	}

	// Commit the target first.
	if err := targetTx.Commit(); err != nil {
		return hlc.Zero(), false, errors.WithStack(err)
	}

	// Without X/A transactions, we are in a vulnerable state here. If
	// the staging transaction fails to commit, however, we'd re-apply
	// the work that was just performed. This should wind up being a
	// no-op in the general case.
	if err := stagingTx.Commit(ctx); err != nil {
		skew.Inc()
		return hlc.Zero(), false, errors.Wrapf(err, "Serial.sweepOnce: skew condition")
	}

	log.Tracef("Serial.sweepOnce: committed %s (%d mutations, %d timestamps)",
		group, workCount, len(work.Data))
	applied.Add(float64(workCount))
	duration.Observe(time.Since(start).Seconds())
	success.SetToCurrentTime()

	// If there's potentially more work to do, we can only guarantee
	// we've processed up to the point at which the query would resume.
	// However, if we know that we've consumed all possible data, we can
	// jump ahead to the end of the time range.
	if moreWork {
		return q.StartAt, true, nil
	}
	return q.EndBefore, false, nil
}
