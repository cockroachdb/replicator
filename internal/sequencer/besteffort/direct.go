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

package besteffort

import (
	"context"
	"time"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/lockset"
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// directAcceptor attempts to write incoming mutations directly to the
// target database or will delegate them.
type directAcceptor struct {
	*BestEffort
	apply    types.MultiAcceptor
	fallback types.MultiAcceptor
}

var _ types.TableAcceptor = (*directAcceptor)(nil)

// AcceptMultiBatch aggregates mutations by table.
func (a *directAcceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	byTable := types.FlattenByTable[*types.MultiBatch](batch)
	return byTable.Range(func(table ident.Table, muts []types.Mutation) error {
		return a.AcceptTableBatch(ctx, &types.TableBatch{
			Data:  muts,
			Table: table,
		}, opts)
	})
}

// AcceptTableBatch implements [types.TableAcceptor].
func (a *directAcceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	start := time.Now()
	tblValues := metrics.TableValues(batch.Table)
	appliedCount := acceptAppliedCount.WithLabelValues(tblValues...)
	deferredCount := acceptDeferredCount.WithLabelValues(tblValues...)
	duration := acceptDuration.WithLabelValues(tblValues...)
	errCount := acceptErrors.WithLabelValues(tblValues...)

	stager, err := a.stagers.Get(ctx, batch.Table)
	if err != nil {
		return err
	}

	// Perform work via the scheduler to ensure we can't step on
	// anyone's toes.
	outcome := a.scheduler.TableBatch(batch, func() error {
		stageTx, err := a.stagingPool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = stageTx.Rollback(ctx) }()

		// If a key already has a pending mutation, we want to stage the
		// next mutation for the key. This call will return the mutations
		// that we still want to attempt to apply.
		attempt, err := stager.StageIfExists(ctx, stageTx, batch.Data)
		if err != nil {
			return err
		}
		deferredCount.Add(float64(len(batch.Data) - len(attempt)))
		mustCommit := len(attempt) < len(batch.Data)

		// Further debounce any mutations in the case of non-idempotent
		// sources.
		if !a.cfg.IdempotentSource {
			attempt, err = stager.FilterApplied(ctx, stageTx, attempt)
			if err != nil {
				return err
			}
		}

		// No mutations to apply, but we may need to commit the staging
		// transaction here if the StageIfExists call wrote data.
		if len(attempt) == 0 {
			if mustCommit {
				return errors.WithStack(stageTx.Commit(ctx))
			}
			return nil
		}

		// Apply the remaining mutations. This call disregards the
		// original options, since we wouldn't want to use any
		// pre-existing transaction.
		attemptBatch := batch.Empty()
		attemptBatch.Data = attempt
		if err = a.apply.AcceptTableBatch(ctx, attemptBatch, &types.AcceptOptions{}); err != nil {
			return err
		}

		// Record the completion of this batch.
		if err := stager.MarkApplied(ctx, stageTx, attempt); err != nil {
			return err
		}

		// Commit the staging transaction.
		if err := stageTx.Commit(ctx); err != nil {
			return errors.WithStack(err)
		}

		appliedCount.Add(float64(len(attempt)))
		duration.Observe(time.Since(start).Seconds())
		return nil
	})

	if err := lockset.Wait(ctx, []lockset.Outcome{outcome}); err != nil {
		// We've encountered some error that may affect one or all of
		// the mutations in the batch. We'll pass the original batch up
		// the original sequencer's acceptor (where it will most likely
		// just be written to staging).
		errCount.Inc()
		log.WithError(err).Tracef(
			"could not apply mutations to %s; using fallback", batch.Table)
		return a.fallback.AcceptTableBatch(ctx, batch, opts)
	}
	return nil
}

// AcceptTemporalBatch aggregates mutations by table.
func (a *directAcceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	byTable := types.FlattenByTable[*types.TemporalBatch](batch)
	return byTable.Range(func(table ident.Table, muts []types.Mutation) error {
		return a.AcceptTableBatch(ctx, &types.TableBatch{
			Data:  muts,
			Table: table,
		}, opts)
	})
}
