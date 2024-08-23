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

package core

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/lockset"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// A round contains the workflow necessary to commit a batch of
// mutations to the target database.
type round struct {
	*Core

	// Initialized by caller.
	delegate types.MultiAcceptor
	group    *types.TableGroup
	poisoned *poisonSet

	// The last time within the accumulated data. This is used for
	// partial progress reporting.
	advanceTo hlc.Range
	// Accumulates (multi-segment) tasks. This represents a potential
	// point of memory exhaustion; we may want to be able to spill this
	// to disk. We need to keep this information available in order to
	// be able to retry in case of 40001, etc. errors.
	batch          *types.MultiBatch
	mutationCount  int
	timestampCount int

	// Metrics
	applied     prometheus.Counter
	duration    prometheus.Observer
	lastAttempt prometheus.Gauge
	lastSuccess prometheus.Gauge
	skew        prometheus.Counter
}

func (r *round) accumulate(segment *types.MultiBatch) error {
	r.mutationCount += segment.Count()
	for _, temp := range segment.Data {
		r.timestampCount++
		if hlc.Compare(temp.Time, r.advanceTo.MaxInclusive()) > 0 {
			r.advanceTo = hlc.RangeExcluding(hlc.Zero(), temp.Time)
		}
	}
	if r.batch == nil {
		r.batch = segment
		return nil
	}
	return segment.CopyInto(r.batch)
}

// scheduleCommit handles the error-retry logic around tryCommit.
func (r *round) scheduleCommit(
	ctx context.Context, progressReport chan<- hlc.Range,
) lockset.Outcome {
	scheduledAt := time.Now()
	return r.Core.scheduler.Batch(r.batch, func() error {
		defer close(progressReport)

		// If the batch touches already-poisoned keys, do nothing. This
		// method has a side effect of contaminating all keys in the
		// batch to ensure correct dependencies.
		if r.poisoned.IsPoisoned(r.batch) {
			return errPoisoned
		}

		// Internally retry 40001, etc. errors.
		err := retry.Retry(ctx, r.targetPool, func(ctx context.Context) error {
			return r.tryCommit(ctx)
		})

		// Report successful progress.
		if err == nil {
			progressReport <- r.advanceTo

			log.Tracef("commited %s (%d mutations, %d timestamps) to %s",
				r.group, r.mutationCount, r.timestampCount, r.advanceTo)
			r.applied.Add(float64(r.mutationCount))
			r.duration.Observe(time.Since(scheduledAt).Seconds())
			r.lastSuccess.SetToCurrentTime()

			return nil
		}

		// Give the keys in the batch a second chance to be applied if
		// the initial error is an FK violation. If the keys can't be
		// retried, mark them as poisoned.
		if r.targetPool.IsDeferrable(err) && r.poisoned.maxCount > 0 {
			return retry.Retry(ctx, r.targetPool, func(ctx context.Context) error {
				count, toPoison, err := r.tryCommitPartial(ctx)
				if err != nil {
					return err
				}
				r.poisoned.Merge(toPoison)

				log.Tracef("partially commited %s (%d of %d mutations, %d timestamps) to %s",
					r.group, count, r.mutationCount, r.timestampCount, r.advanceTo)
				r.applied.Add(float64(count))
				r.duration.Observe(time.Since(scheduledAt).Seconds())
				r.lastSuccess.SetToCurrentTime()

				return nil
			})
		}

		// General error case: poison the keys.
		r.poisoned.MarkPoisoned(r.batch)
		return err
	})
}

// tryCommit attempts to commit the batch. It will send the data to the
// target and mark the mutations as applied within staging.
func (r *round) tryCommit(ctx context.Context) error {
	r.lastAttempt.SetToCurrentTime()
	var err error

	// Passed to staging.
	toMark := ident.TableMap[[]types.Mutation]{}
	_ = r.batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		toMark.Put(table, append(toMark.GetZero(table), mut))
		return nil
	}))

	log.Tracef("round.tryCommit: beginning tx for %s to %s", r.group, r.advanceTo)

	// We're going to manage the target and the staging transaction
	// concurrently. The target transaction must commit before the
	// staging transaction.
	stagingTx, err := r.stagingPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = stagingTx.Rollback(context.Background()) }()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		targetTx, err := r.targetPool.BeginTx(ctx, nil)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = targetTx.Rollback() }()
		if err := r.delegate.AcceptMultiBatch(egCtx, r.batch, &types.AcceptOptions{
			TargetQuerier: targetTx,
		}); err != nil {
			return err
		}
		return errors.WithStack(targetTx.Commit())
	})
	eg.Go(func() error {
		return toMark.Range(func(table ident.Table, muts []types.Mutation) error {
			stager, err := r.stagers.Get(ctx, table)
			if err != nil {
				return err
			}
			return stager.MarkApplied(ctx, stagingTx, muts)
		})
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	// Without X/A transactions, we are in a vulnerable
	// state here. If the staging transaction fails to
	// commit, however, we'd re-apply the work that was just
	// performed. This should wind up being a no-op in the
	// general case.
	err = errors.WithStack(stagingTx.Commit(ctx))
	if err != nil {
		r.skew.Inc()
		return errors.Wrap(err, "round.tryCommit: skew condition")
	}

	return nil
}

// tryCommitPartial attempts to commit the batch, one mutation at a
// time, using savepoints. This method will allow mutations for an
// individual row to be skipped over if a later mutation can be applied.
// The number of successfully-applied rows is returned, along with
// a poisonSet containing the mutations that could not be applied.
func (r *round) tryCommitPartial(ctx context.Context) (int, *poisonSet, error) {
	r.lastAttempt.SetToCurrentTime()

	log.Tracef("round.tryCommitPartial: beginning tx for %s to %s", r.group, r.advanceTo)
	poisoned := newPoisonSet(math.MaxInt)

	targetTx, err := r.targetPool.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, errors.WithStack(err)
	}
	defer func() { _ = targetTx.Rollback() }()

	if err := r.batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		_, err := targetTx.Exec("SAVEPOINT replicator")
		if err != nil {
			return errors.WithStack(err)
		}
		err = r.delegate.AcceptTableBatch(ctx,
			&types.TableBatch{
				Data:  []types.Mutation{mut},
				Table: table,
			},
			&types.AcceptOptions{TargetQuerier: targetTx})

		// Record the mutation to be marked later. We've been able to
		// skip ahead, so remove the poison marker from the row.
		if err == nil {
			poisoned.RemoveMark(table, mut)
			return nil
		}

		// Ignore deferrable error by rolling back to savepoint. We'll
		// mark the individual mutation as being poisoned to filter is
		// later.
		if r.targetPool.IsDeferrable(err) {
			poisoned.MarkMutation(table, mut)
			_, err = targetTx.Exec("ROLLBACK TO SAVEPOINT replicator")
			err = errors.WithStack(err)
		}
		return err
	})); err != nil {
		return 0, nil, err
	}

	if err := targetTx.Commit(); err != nil {
		return 0, nil, errors.WithStack(err)
	}

	// Mark all non-poisoned mutations as complete.
	toMark := ident.TableMap[[]types.Mutation]{}
	_ = r.batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		if !poisoned.IsMutationPoisoned(table, mut) {
			toMark.Put(table, append(toMark.GetZero(table), mut))
		}
		return nil
	}))

	// We're going to manage the target and the staging transaction
	// concurrently. The target transaction must commit before the
	// staging transaction. Without X/A transactions, we are in a
	// vulnerable state here. If the staging transaction fails to
	// commit, however, we'd re-apply the work that was just performed.
	// This should wind up being a no-op in the general case.
	didSkew := true
	stagingTx, err := r.stagingPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, nil, errors.WithStack(err)
	}
	defer func() {
		_ = stagingTx.Rollback(context.Background())
		if didSkew {
			r.skew.Inc()
		}
	}()

	markCount := 0
	if err := toMark.Range(func(table ident.Table, muts []types.Mutation) error {
		markCount += len(muts)
		stager, err := r.stagers.Get(ctx, table)
		if err != nil {
			return err
		}
		return stager.MarkApplied(ctx, stagingTx, muts)
	}); err != nil {
		return 0, nil, err
	}

	if err := stagingTx.Commit(ctx); err != nil {
		return 0, nil, errors.WithStack(err)
	}
	didSkew = false

	return markCount, poisoned, nil
}
