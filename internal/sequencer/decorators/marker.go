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
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
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

// AcceptMultiBatch marks values concurrently with calling the
// underlying decorator.
func (s *marker) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	if err := s.multiAcceptor.AcceptMultiBatch(ctx, batch, opts); err != nil {
		return err
	}

	return s.mark(ctx, types.FlattenByTable[*types.MultiBatch](batch))
}

func (s *marker) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if err := s.tableAcceptor.AcceptTableBatch(ctx, batch, opts); err != nil {
		return err
	}

	return s.mark(ctx, types.FlattenByTable[*types.TableBatch](batch))
}

func (s *marker) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	if err := s.temporalAcceptor.AcceptTemporalBatch(ctx, batch, opts); err != nil {
		return err
	}

	return s.mark(ctx, types.FlattenByTable[*types.TemporalBatch](batch))
}

func (s *marker) mark(ctx context.Context, flattened *ident.TableMap[[]types.Mutation]) error {
	return retry.Retry(ctx, s.stagingPool, func(ctx context.Context) error {
		tx, err := s.stagingPool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()

		if err := flattened.Range(func(table ident.Table, muts []types.Mutation) error {
			stager, err := s.stagers.Get(ctx, table)
			if err != nil {
				return err
			}

			return stager.MarkApplied(ctx, s.stagingPool, muts)
		}); err != nil {
			return err
		}

		return errors.WithStack(tx.Commit(ctx))
	})
}
