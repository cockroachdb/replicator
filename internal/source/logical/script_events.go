// Copyright 2023 The Cockroach Authors
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

package logical

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// scriptEvents wraps an Events implementation to allow a user-script
// to intercept and dispatch mutations.
type scriptEvents struct {
	Events
	Script *script.UserScript
}

var _ Events = (*scriptEvents)(nil)

// OnBegin implements Events.
func (e *scriptEvents) OnBegin(ctx context.Context) (Batch, error) {
	delegate, err := e.Events.OnBegin(ctx)
	if err != nil {
		return nil, err
	}
	return &scriptBatch{delegate, e.Script}, nil
}

type scriptBatch struct {
	Batch
	Script *script.UserScript
}

var _ Batch = (*scriptBatch)(nil)

// OnData implements Batch and calls any mapping logic provided by the
// user-script for the given table.
func (e *scriptBatch) OnData(
	ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	// If we see any deletes, we need to know where to send them to
	// (e.g. to use ON DELETE CASADE). Depending on the source, there
	// may or may not be an obvious table for deletes.
	deletesTo := target

	// If there is a configuration and a dispatch function defined, then
	// call the user-provided logic.
	var routing ident.TableMap[[]types.Mutation]

	if cfg, ok := e.Script.Sources.Get(source); !ok {
		// If no call to configureSource() has been made for this
		// schema, we'll send the mutations to the existing target.
		routing.Put(target, muts)
	} else if dispatch := cfg.Dispatch; dispatch == nil {
		// If no dispatch function has been defined for the schema,
		// well send the mutations to the existing target.
		routing.Put(target, muts)
	} else {
		// We have a dispatch function for the schema. We'll call it to
		// provide the routing for each incoming mutation.

		// The schema configuration may override the default destination
		// for deletes.
		if !cfg.DeletesTo.Empty() {
			deletesTo = cfg.DeletesTo
		}
		// Dispatch each mutation and route the result.
		for _, mut := range muts {
			// Deletes are always routed to the configured (or implied)
			// deletion table.
			if mut.IsDelete() {
				if deletesTo.Empty() {
					return errors.Errorf(
						"cannot apply delete from %s because there is no "+
							"table configured for receiving the delete", source)
				}
				routing.Put(deletesTo, append(routing.GetZero(deletesTo), mut))
				continue
			}
			// Invoke the dispatch function.
			fanOut, err := dispatch(ctx, mut)
			if err != nil {
				return err
			}
			// Append the mutation to the routing map. Ignoring error
			// since the callback returns nil.
			_ = fanOut.Range(func(tbl ident.Table, tblMuts []types.Mutation) error {
				routing.Put(tbl, append(routing.GetZero(tbl), tblMuts...))
				return nil
			})
		}
	}

	// We have established which mutations are going to which table. Now
	// we can take the second step of calling the per-table map
	// function, if any.
	return routing.Range(func(tbl ident.Table, tblMuts []types.Mutation) error {
		// Find the per-target map function.
		var mapFn script.Map
		if cfg, ok := e.Script.Targets.Get(tbl); ok {
			mapFn = cfg.Map
		}

		// Fast-path: No map, so we can just send the data as-is.
		if mapFn == nil {
			return e.Batch.OnData(ctx, source, tbl, tblMuts)
		}

		// We want to separate out the deletes, since they don't make
		// sense to send to the mapper function. This loop uses the
		// slice-filter trick.
		var deletes []types.Mutation
		var idx int
		for _, mut := range tblMuts {
			// Deletes are filtered out.
			if mut.IsDelete() {
				deletes = append(deletes, mut)
				continue
			}

			// Call the map function. It may choose to return no data,
			// in which case we'll filter out the mutation.
			mut, ok, err := mapFn(ctx, mut)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}

			tblMuts[idx] = mut
			idx++
		}
		tblMuts = tblMuts[:idx]
		if len(tblMuts) > 0 {
			if err := e.Batch.OnData(ctx, source, tbl, tblMuts); err != nil {
				return err
			}
		}
		if len(deletes) > 0 {
			if err := e.Batch.OnData(ctx, source, deletesTo, deletes); err != nil {
				return err
			}
		}
		return nil
	})
}
