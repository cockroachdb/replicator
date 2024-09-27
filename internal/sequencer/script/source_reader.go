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

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// A sourceReader is responsible for the actions wired up to a
// configureSource() api call. Specifically, the sourceReader is
// concerned with routing incoming mutations to the correct staging
// and/or target table(s).
type sourceReader struct {
	delegate       types.BatchReader
	group          *types.TableGroup
	sourceBindings *script.Source
	watcher        types.Watcher
}

var _ types.BatchReader = (*sourceReader)(nil)

// Read implements [types.BatchReader].
func (a *sourceReader) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	source, err := a.delegate.Read(ctx)
	if err != nil {
		return nil, err
	}

	out := make(chan *types.BatchCursor, 2)
	ctx.Go(func(ctx *stopper.Context) error {
		defer close(out)
		for {
			var cur *types.BatchCursor
			select {
			case cur = <-source:
			case <-ctx.Stopping():
				return nil
			}

			// Upstream closed due to stopping.
			if cur == nil {
				return nil
			}

			// Only process non-error data payloads.
			if cur.Batch != nil && cur.Error == nil {
				nextBatch := cur.Batch.Empty()
				for table, mut := range cur.Batch.Mutations() {
					if err := a.acceptOne(ctx, nextBatch, table, mut); err != nil {
						return err
					}
				}
				cur.Batch = nextBatch
			}

			// Send downstream.
			select {
			case out <- cur:
			case <-ctx.Stopping():
				return nil
			}
		}
	})

	return out, nil
}

func (a *sourceReader) acceptOne(
	ctx context.Context, acc *types.TemporalBatch, table ident.Table, mutToDispatch types.Mutation,
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
	for table, muts := range dispatched.All() {
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
			dispatchedMut.Time = acc.Time
			if err := acc.Accumulate(table, dispatchedMut); err != nil {
				return errors.Wrap(err, a.group.Name.Raw())
			}
		}
	}
	return nil
}
