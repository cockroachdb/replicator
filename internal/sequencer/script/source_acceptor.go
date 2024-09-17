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

package script

import (
	"context"

	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// A sourceAcceptor is responsible for the actions wired up to a
// configureSource() api call. Specifically, the sourceAcceptor is
// concerned with routing incoming mutations to the correct
// staging and/or target table.
type sourceAcceptor struct {
	delegate       types.MultiAcceptor
	group          *types.TableGroup
	sourceBindings *script.Source
	watcher        types.Watcher
}

var _ types.MultiAcceptor = (*sourceAcceptor)(nil)

func (a *sourceAcceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	return acceptBatch(ctx, a, batch, opts)
}

func (a *sourceAcceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	return acceptBatch(ctx, a, batch, opts)
}

func (a *sourceAcceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	return acceptBatch(ctx, a, batch, opts)
}

// acceptBatch wants to be a generic method.
func acceptBatch[B types.Batch[B]](
	ctx context.Context, a *sourceAcceptor, batch B, opts *types.AcceptOptions,
) error {
	nextBatch := &types.MultiBatch{}

	if err := batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		return a.acceptOne(ctx, nextBatch, table, mut)
	})); err != nil {
		return err
	}

	// Calls to source.Dispatch may remove mutations.
	// If the replacement batch is empty, we are done.
	if nextBatch.Count() == 0 {
		return nil
	}

	return a.delegate.AcceptMultiBatch(ctx, nextBatch, opts)
}

func (a *sourceAcceptor) acceptOne(
	ctx context.Context, acc *types.MultiBatch, table ident.Table, mutToDispatch types.Mutation,
) error {
	script.AddMeta(a.group.Name.Raw(), table, &mutToDispatch)

	isDelete := mutToDispatch.IsDelete()

	dispatch := a.sourceBindings.Dispatch
	fnName := "dispatch"
	if isDelete {
		// Same underlying func signature.
		dispatch = script.Dispatch(a.sourceBindings.DeletesTo)
		fnName = "deletesTo"
	}

	// Call the user function to see what mutations(s) go into which table(s).
	dispatched, err := dispatch(ctx, table, mutToDispatch)
	if err != nil {
		return err
	}
	// Push the mutations into the replacement batch.
	if err := dispatched.Range(func(table ident.Table, muts []types.Mutation) error {
		if table.Empty() {
			return errors.Errorf("%s returned an empty table name", fnName)
		}
		if _, found := a.watcher.Get().Columns.Get(table); !found {
			return errors.Errorf(
				"%s returned a table (%s) which does not exist in the target schema",
				fnName, table)
		}
		for _, dispatchedMut := range muts {
			dispatchedMut.Deletion = isDelete
			// Preserve incoming timestamp.
			dispatchedMut.Time = mutToDispatch.Time
			if err := acc.Accumulate(table, dispatchedMut); err != nil {
				return err
			}
		}
		return err
	}); err != nil {
		return errors.Wrap(err, a.group.Name.Raw())
	}
	return nil
}
