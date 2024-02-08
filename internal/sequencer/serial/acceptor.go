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

package serial

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

type acceptor struct {
	*Serial
}

var _ types.MultiAcceptor = (*acceptor)(nil)

// AcceptMultiBatch implements [types.MultiAcceptor] and processes the
// batch in time order.
func (a *acceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	// Coalesce for better database interaction.
	var mutsByTable ident.TableMap[[]types.Mutation]
	for _, sub := range batch.Data {
		// No error returned in callback.
		_ = sub.Data.Range(func(tbl ident.Table, tblBatch *types.TableBatch) error {
			mutsByTable.Put(tbl, append(mutsByTable.GetZero(tbl), tblBatch.Data...))
			return nil
		})
	}

	tx := types.StagingQuerier(a.stagingPool)
	if opts != nil && opts.StagingQuerier != nil {
		tx = opts.StagingQuerier
	}

	return mutsByTable.Range(func(tbl ident.Table, muts []types.Mutation) error {
		stager, err := a.stagers.Get(ctx, tbl)
		if err != nil {
			return err
		}
		return stager.Store(ctx, tx, muts)
	})
}

// AcceptTableBatch implements [types.TableAcceptor].
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	tx := types.StagingQuerier(a.stagingPool)
	if opts != nil && opts.StagingQuerier != nil {
		tx = opts.StagingQuerier
	}

	stager, err := a.stagers.Get(ctx, batch.Table)
	if err != nil {
		return err
	}
	return stager.Store(ctx, tx, batch.Data)
}

// AcceptTemporalBatch implements [types.MultiAcceptor]. This does not
// impose any per-table ordering, since the staging tables have no order
// requirements.
func (a *acceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	multi := &types.MultiBatch{
		Data:   []*types.TemporalBatch{batch},
		ByTime: map[hlc.Time]*types.TemporalBatch{batch.Time: batch},
	}
	return a.AcceptMultiBatch(ctx, multi, opts)
}
