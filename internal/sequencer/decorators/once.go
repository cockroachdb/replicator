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
	"github.com/pkg/errors"
)

// Once constructs facades around acceptors that filters out mutations
// which have already been marked as having been applied by calling
// [types.Stager.FilterApplied].
type Once struct {
	pool    *types.StagingPool
	stagers types.Stagers
}

// MultiAcceptor returns a deduplicating facade around the delegate.
func (o *Once) MultiAcceptor(acceptor types.MultiAcceptor) types.MultiAcceptor {
	return &once{
		base: base{
			multiAcceptor:    acceptor,
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		Once: o,
	}
}

// TableAcceptor returns a deduplicating facade around the delegate.
func (o *Once) TableAcceptor(acceptor types.TableAcceptor) types.TableAcceptor {
	return &once{
		base: base{
			tableAcceptor: acceptor,
		},
		Once: o,
	}
}

// TemporalAcceptor returns a deduplicating facade around the delegate.
func (o *Once) TemporalAcceptor(acceptor types.TemporalAcceptor) types.TemporalAcceptor {
	return &once{
		base: base{
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		Once: o,
	}
}

type once struct {
	base
	*Once
}

var _ types.MultiAcceptor = (*once)(nil)

func (o *once) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	if o.multiAcceptor == nil {
		return errors.New("no multiAcceptor set")
	}
	filtered, err := o.filterMultiBatch(ctx, batch)
	if err != nil {
		return err
	}
	return o.multiAcceptor.AcceptMultiBatch(ctx, filtered, opts)
}

func (o *once) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if o.tableAcceptor == nil {
		return errors.New("no tableAcceptor set")
	}
	filtered, err := o.filterTableBatch(ctx, batch)
	if err != nil {
		return err
	}
	return o.tableAcceptor.AcceptTableBatch(ctx, filtered, opts)
}

func (o *once) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	if o.temporalAcceptor == nil {
		return errors.New("no temporalAcceptor set")
	}
	filtered, err := o.filterTemporalBatch(ctx, batch)
	if err != nil {
		return err
	}
	return o.temporalAcceptor.AcceptTemporalBatch(ctx, filtered, opts)
}

func (o *once) filterMultiBatch(
	ctx context.Context, batch *types.MultiBatch,
) (*types.MultiBatch, error) {
	// Since a MultiBatch will likely refer to the same table multiple
	// times, we'll flatten the batch to reduce the total number of
	// database queries performed.
	flattened := types.FlattenByTable(batch)
	thinned := batch.Empty()
	for table, muts := range flattened.All() {
		stager, err := o.stagers.Get(ctx, table)
		if err != nil {
			return nil, err
		}
		filtered, err := stager.FilterApplied(ctx, o.pool, muts)
		if err != nil {
			return nil, err
		}
		// If all mutations have been filtered out, do nothing.
		if len(filtered) == 0 {
			continue
		}
		for _, mut := range filtered {
			if err := thinned.Accumulate(table, mut); err != nil {
				return nil, err
			}
		}
	}

	return thinned, nil
}

func (o *once) filterTableBatch(
	ctx context.Context, batch *types.TableBatch,
) (*types.TableBatch, error) {
	stager, err := o.stagers.Get(ctx, batch.Table)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Filter any mutations that have already been applied.
	next := batch.Empty()
	next.Data, err = stager.FilterApplied(ctx, o.pool, batch.Data)
	if err != nil {
		return nil, err
	}
	return next, nil
}

func (o *once) filterTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch,
) (*types.TemporalBatch, error) {
	next := batch.Empty()
	for table, tableBatch := range batch.Data.All() {
		nextTableBatch, err := o.filterTableBatch(ctx, tableBatch)
		if err != nil {
			return nil, err
		}
		next.Data.Put(table, nextTableBatch)
	}
	return next, nil
}
