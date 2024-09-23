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

package decorators

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Marker adds a decorator which will marks all mutations as having been
// applied via [types.Stager.MarkApplied].
type Marker struct {
	stagers     types.Stagers
	stagingPool *types.StagingPool
}

// MultiAcceptor returns a marking facade around the delegate.
func (r *Marker) MultiAcceptor(acceptor types.MultiAcceptor) types.MultiAcceptor {
	return &marker{
		base: base{
			multiAcceptor:    acceptor,
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		Marker: r,
	}
}

// TableAcceptor returns a marking facade around the delegate.
func (r *Marker) TableAcceptor(acceptor types.TableAcceptor) types.TableAcceptor {
	return &marker{
		base: base{
			tableAcceptor: acceptor,
		},
		Marker: r,
	}
}

// TemporalAcceptor returns a marking facade around the delegate.
func (r *Marker) TemporalAcceptor(acceptor types.TemporalAcceptor) types.TemporalAcceptor {
	return &marker{
		base: base{
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		Marker: r,
	}
}

type marker struct {
	base
	*Marker
}

var _ types.MultiAcceptor = (*marker)(nil)

func (s *marker) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	return acceptMark(ctx, s.stagingPool, s.stagers, batch, opts, s.multiAcceptor.AcceptMultiBatch)
}

func (s *marker) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	return acceptMark(ctx, s.stagingPool, s.stagers, batch, opts, s.tableAcceptor.AcceptTableBatch)
}

func (s *marker) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	return acceptMark(ctx, s.stagingPool, s.stagers, batch, opts, s.temporalAcceptor.AcceptTemporalBatch)
}

// acceptMark will execute the callback concurrently with marking the
// batch's mutations as applied. The callback must complete successfully
// before the marking transaction is committed.
func acceptMark[B types.Batch[B]](
	ctx context.Context,
	pool *types.StagingPool,
	stagers types.Stagers,
	batch B,
	opts *types.AcceptOptions,
	fn func(ctx context.Context, batch B, opts *types.AcceptOptions) error,
) error {
	eg, egCtx := errgroup.WithContext(ctx)

	// Pass work up the chain.
	eg.Go(func() error { return fn(egCtx, batch, opts) })

	// Prepare a transaction to mark the mutations.
	var stagingTx pgx.Tx
	defer func() {
		if stagingTx != nil {
			_ = stagingTx.Rollback(context.Background())
		}
	}()

	// Pre-stage the mutations.
	eg.Go(func() error {
		flattened := types.FlattenByTable(batch)

		return retry.Retry(egCtx, pool, func(retryCtx context.Context) error {
			var err error
			stagingTx, err = pool.BeginTx(retryCtx, pgx.TxOptions{})
			if err != nil {
				return errors.WithStack(err)
			}

			for table, muts := range flattened.All() {
				stager, err := stagers.Get(retryCtx, table)
				if err != nil {
					return err
				}

				if err := stager.MarkApplied(retryCtx, stagingTx, muts); err != nil {
					return err
				}
			}

			return nil
		})
	})

	// Ensure the delegate committed and the staging transaction is ready.
	if err := eg.Wait(); err != nil {
		return err
	}

	if err := stagingTx.Commit(ctx); err != nil {
		stagingSkewCount.Inc()
		return errors.WithStack(err)
	}
	return nil
}
