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

package shingle

import (
	"context"
	"database/sql"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/lockset"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

type acceptor struct {
	*Shingle
	delegate types.MultiAcceptor
}

var (
	_ sequencer.MarkingAcceptor = (*acceptor)(nil)
	_ types.MultiAcceptor       = (*acceptor)(nil)
)

// AcceptTableBatch implements [types.MultiAcceptor]. It is not expected
// to be called in the general case.
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	temp := &types.TemporalBatch{Time: batch.Time}
	temp.Data.Put(batch.Table, batch)
	return a.AcceptTemporalBatch(ctx, temp, opts)
}

// AcceptTemporalBatch implements [types.TemporalAcceptor]. It is not
// expected // to be called in the general case.
func (a *acceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	multi := &types.MultiBatch{
		ByTime: map[hlc.Time]*types.TemporalBatch{batch.Time: batch},
		Data:   []*types.TemporalBatch{batch},
	}
	return a.AcceptMultiBatch(ctx, multi, opts)
}

// AcceptMultiBatch executes each enclosed TemporalBatch in a concurrent
// fashion. Two batches may be executed concurrently if they have no
// overlapping primary keys.
func (a *acceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	// Break the incoming batch up into reasonably-sized chunks. We want
	// to avoid sending hundreds of trivially-small transactions to the
	// target.
	segments := segmentMultiBatch(batch, a.cfg.FlushSize)

	stopCtx := stopper.From(ctx)

	outcomes := make([]*notify.Var[*lockset.Status], len(segments))
	for idx, segment := range segments {
		idx, segment := idx, segment // Capture
		outcomes[idx] = a.scheduler.Batch(segment, func() error {
			// We may have been delayed for some time, so we'll re-check
			// that it's OK to continue running.
			if stopCtx.IsStopping() {
				return context.Canceled
			}

			tx, err := a.target.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				return errors.WithStack(err)
			}
			defer func() { _ = tx.Rollback() }()

			opts := opts.Copy()
			opts.TargetQuerier = tx
			if err := a.delegate.AcceptMultiBatch(ctx, segment, opts); err != nil {
				return err
			}

			if err := tx.Commit(); err != nil {
				return errors.WithStack(err)
			}

			// Accumulate all mutations that were applied.
			var applied ident.TableMap[[]types.Mutation]
			for _, sub := range segment.Data {
				if err := sub.Data.Range(
					func(table ident.Table, tableBatch *types.TableBatch) error {
						applied.Put(table, append(applied.GetZero(table), tableBatch.Data...))
						return nil
					}); err != nil {
					return err
				}
			}

			// Mark the mutations as having been applied.
			stagingTx, err := a.staging.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return errors.WithStack(err)
			}
			defer func() { _ = stagingTx.Rollback(context.Background()) }()

			if err := applied.Range(func(table ident.Table, muts []types.Mutation) error {
				stager, err := a.stagers.Get(ctx, table)
				if err != nil {
					return err
				}
				return stager.MarkApplied(ctx, stagingTx, muts)
			}); err != nil {
				return err
			}

			return errors.WithStack(stagingTx.Commit(ctx))
		})
	}

	// Await completion of tasks.
outer:
	for _, outcome := range outcomes {
		for {
			status, changed := outcome.Get()
			if status.Success() {
				continue outer
			} else if status.Err() != nil {
				return status.Err()
			}
			select {
			case <-changed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// IsMarking implements [sequencer.MarkingAcceptor].
func (a *acceptor) IsMarking() bool { return true }

// Unwrap is an informal protocol to return the delegate.
func (a *acceptor) Unwrap() types.MultiAcceptor {
	return a.delegate
}
