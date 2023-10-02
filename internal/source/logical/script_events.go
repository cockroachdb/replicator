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
		routing.Put(target, muts)
	} else if dispatcher := cfg.Dispatch; dispatcher == nil {
		routing.Put(target, muts)
	} else {
		// The configuration may override the default destination for
		// deletes.
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

			fanOut, err := dispatcher(ctx, mut)
			if err != nil {
				return err
			}
			// Ignoring error since the callback returns nil.
			_ = fanOut.Range(func(tbl ident.Table, tblMuts []types.Mutation) error {
				routing.Put(tbl, append(routing.GetZero(tbl), tblMuts...))
				return nil
			})
		}
	}

	// Now that we have established which mutations are going to which
	// destination table, we want to separate out the deletes, since
	// they don't make sense to send to the mapper function.
	return routing.Range(func(tbl ident.Table, tblMuts []types.Mutation) error {
		var mapper script.Map
		if cfg, ok := e.Script.Targets.Get(tbl); ok {
			mapper = cfg.Map
		}

		// Fast-path: No mapper, so we can just send the data as-is.
		if mapper == nil {
			return e.Batch.OnData(ctx, source, tbl, tblMuts)
		}

		// Filter slice trick. We'll extract deletions and possibly
		// discard mutations per the mapper function.
		var deletes []types.Mutation
		var idx int
		for _, mut := range tblMuts {
			if mut.IsDelete() {
				deletes = append(deletes, mut)
				continue
			}

			mut, ok, err := mapper(ctx, mut)
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

// sendToTarget applies any per-target logic in the user-script and
// then delegates to Events.OnData.
func (e *scriptBatch) sendToTarget(
	ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	cfg, ok := e.Script.Targets.Get(target)
	if !ok {
		return e.Batch.OnData(ctx, source, target, muts)
	}
	mapperFn := cfg.Map
	if mapperFn == nil {
		return e.Batch.OnData(ctx, source, target, muts)
	}

	// Filter with replacement.
	idx := 0
	for _, mut := range muts {
		mut, ok, err := mapperFn(ctx, mut)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		muts[idx] = mut
		idx++
	}
	if idx == 0 {
		return nil
	}
	muts = muts[:idx]
	return e.Batch.OnData(ctx, source, target, muts)
}
