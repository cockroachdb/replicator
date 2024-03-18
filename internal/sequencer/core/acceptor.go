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

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
)

// An acceptor writes incoming data to staging.
type acceptor struct {
	*Core
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

	return mutsByTable.Range(func(tbl ident.Table, muts []types.Mutation) error {
		stager, err := a.stagers.Get(ctx, tbl)
		if err != nil {
			return err
		}
		return retry.Retry(ctx, a.stagingPool, func(ctx context.Context) error {
			return stager.Stage(ctx, a.stagingPool, muts)
		})
	})
}

// AcceptTableBatch implements [types.TableAcceptor].
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	stager, err := a.stagers.Get(ctx, batch.Table)
	if err != nil {
		return err
	}
	return retry.Retry(ctx, a.stagingPool, func(ctx context.Context) error {
		return stager.Stage(ctx, a.stagingPool, batch.Data)
	})
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
