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
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/lockset"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// A round contains the workflow necessary to commit a batch of
// mutations to the target database.
type round struct {
	*Core

	// Initialized by caller.
	delegate types.MultiAcceptor
	group    *types.TableGroup
	poisoned *poisonSet
	terminal sequencer.TerminalFunc

	// The last time within the accumulated data. This is used for
	// partial progress reporting.
	advanceTo hlc.Range
	// Accumulates (multi-segment) tasks. This represents a potential
	// point of memory exhaustion; we may want to be able to spill this
	// to disk. We need to keep this information available in order to
	// be able to retry in case of 40001, etc. errors.
	batch          *types.MultiBatch
	markers        []any
	mutationCount  int
	timestampCount int

	// Metrics
	applied     prometheus.Counter
	duration    prometheus.Observer
	lastAttempt prometheus.Gauge
	lastSuccess prometheus.Gauge
}

func (r *round) accumulate(cursors []*types.BatchCursor) error {
	for _, cur := range cursors {
		batch := cur.Batch
		r.mutationCount += batch.Count()
		for temp := range batch.Data.Values() {
			r.timestampCount++
			if hlc.Compare(temp.Time, r.advanceTo.MaxInclusive()) > 0 {
				r.advanceTo = hlc.RangeExcluding(hlc.Zero(), temp.Time)
			}
		}
		if err := types.Apply(cur.Batch.Mutations(), r.batch.Accumulate); err != nil {
			return err
		}
		if cur.Marker != nil {
			r.markers = append(r.markers, cur.Marker)
		}
	}
	return nil
}

// scheduleCommit handles the error-retry logic around tryCommit.
func (r *round) scheduleCommit(
	ctx *stopper.Context, progressReport chan<- hlc.Range,
) lockset.Outcome {
	start := time.Now()
	work := func(ctx *stopper.Context) error {
		// We want to close the reporting channel, unless we're asking
		// to be retried.
		finalReport := true
		defer func() {
			if finalReport {
				close(progressReport)
			}
		}()

		// If the batch touches poisoned keys, do nothing. This method
		// has a side effect of contaminating all keys in the batch to
		// ensure correct dependencies.
		if r.poisoned.IsPoisoned(r.batch) {
			return errPoisoned
		}

		// Internally retry 40001, etc. errors.
		err := retry.Retry(ctx, r.targetPool, func(ctx context.Context) error {
			return r.tryCommit(stopper.From(ctx))
		})

		// Report successful progress.
		if err == nil {
			progressReport <- r.advanceTo

			log.Tracef("round.tryCommit: commited %s (%d mutations, %d timestamps) to %s",
				r.group, r.mutationCount, r.timestampCount, r.advanceTo)
			r.applied.Add(float64(r.mutationCount))
			r.duration.Observe(time.Since(start).Seconds())
			r.lastSuccess.SetToCurrentTime()

			return nil
		}

		// Give the keys in the batch a second chance to be applied if
		// the initial error is an FK violation. If the keys can't be
		// retried, mark them as poisoned.
		if r.targetPool.IsDeferrable(err) {
			finalReport = false
			return lockset.RetryAtHead(err).Or(func() {
				r.poisoned.MarkPoisoned(r.batch)
				close(progressReport)
			})
		}

		// General error case: poison the keys.
		r.poisoned.MarkPoisoned(r.batch)
		return err
	}

	// We want to make this asynchronous work visible to any enclosing
	// stoppers. This allows other components to be aware of and wait
	// for background tasks like (relatively) long-running database
	// queries to complete.
	return r.Core.scheduler.Batch(r.batch, func() error {
		return ctx.Call(work)
	})
}

// tryCommit attempts to commit the batch. It will send the data to the
// target and mark the mutations as applied within staging.
func (r *round) tryCommit(ctx *stopper.Context) (err error) {
	// We may have been delayed for an arbitrarily long period of time.
	if ctx.IsStopping() {
		return stopper.ErrStopped
	}
	log.Tracef("round.tryCommit: beginning for %s to %s", r.group, r.advanceTo)

	if fn := r.terminal; fn != nil {
		defer func() {
			var nextErr error
			for _, marker := range r.markers {
				nextErr = fn(ctx, marker, err)
			}
			err = nextErr
		}()
	}

	r.lastAttempt.SetToCurrentTime()

	targetTx, err := r.targetPool.BeginTx(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = targetTx.Rollback() }()
	if err := r.delegate.AcceptMultiBatch(ctx, r.batch, &types.AcceptOptions{
		TargetQuerier: targetTx,
	}); err != nil {
		return err
	}
	if err := targetTx.Commit(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
