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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/lockset"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
			return nil
		})
	}

	// Await completion of tasks.
	if err := lockset.Wait(ctx, outcomes); err != nil {
		return err
	}

	// Mark all mutations in the original batch as having been applied.
	// We need to do this as an atomic unit of work. Consider the case
	// where segments S1 and S2 have a key in common. S1 may have failed
	// to commit, while S2 succeeded. We don't want to mark the
	// mutations comprising S2 as successful yet. Otherwise, when we
	// retry S1 without re-attempting the mutations in S2, time might
	// reverse.
	markingStart := time.Now()
	var toMark ident.TableMap[[]types.Mutation]
	for _, temp := range batch.Data {
		// Callback always returns nil.
		_ = temp.Data.Range(func(table ident.Table, tableBatch *types.TableBatch) error {
			toMark.Put(table, append(toMark.GetZero(table), tableBatch.Data...))
			return nil
		})
	}

	stagingTx, err := a.staging.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = stagingTx.Rollback(context.Background()) }()
	if err := toMark.Range(func(table ident.Table, muts []types.Mutation) error {
		stager, err := a.stagers.Get(ctx, table)
		if err != nil {
			return err
		}
		return stager.MarkApplied(ctx, stagingTx, muts)
	}); err != nil {
		return err
	}
	if err := errors.WithStack(stagingTx.Commit(ctx)); err != nil {
		return err
	}
	log.Tracef("marking finished in %s", time.Since(markingStart))
	return nil
}

// IsMarking implements [sequencer.MarkingAcceptor].
func (a *acceptor) IsMarking() bool { return true }

// Unwrap is an informal protocol to return the delegate.
func (a *acceptor) Unwrap() types.MultiAcceptor {
	return a.delegate
}
