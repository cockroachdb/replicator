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
	"reflect"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

// deferredData saves calls to OnData that need to be re-ordered.
type deferredData struct {
	muts   []types.Mutation
	source ident.Ident
	target ident.Table
}

// orderedEvents provides compatibility with target schemas that
// have foreign keys enabled. It does this by accumulating and sorting
// mutations to satisfy an (acyclic) FK dependency graph.
type orderedEvents struct {
	Events
	Watcher types.Watcher

	// This only contains values for deferred mutations. That is,
	// mutations to be applied to "root" tables will never be added
	// here; they're immediately passed through.
	deferred [][]deferredData
	// Remember the last configuration.
	lastDeps [][]ident.Table
	// The table dependency tree.
	levels map[ident.Table]int
}

// OnBegin implements Events. It will initialize the orderedEvents
// fields in response to updated schema information.
func (e *orderedEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	e.reset()
	deps := e.Watcher.Get().Order
	if !reflect.DeepEqual(deps, e.lastDeps) {
		e.lastDeps = deps
		e.deferred = make([][]deferredData, len(deps)-1)
		for idx := range e.deferred {
			e.deferred[idx] = make([]deferredData, 0, batches.Size())
		}
		e.levels = make(map[ident.Table]int)
		for level, tbls := range deps {
			for _, tbl := range tbls {
				e.levels[tbl] = level
			}
		}
	}
	return errors.Wrap(e.Events.OnBegin(ctx, point), "orderedEvents onBegin")
}

// OnCommit implements Events. It will flush any deferred updates.
func (e *orderedEvents) OnCommit(ctx context.Context) error {
	defer e.reset()
	for _, defs := range e.deferred {
		// Ensure that previous levels have been completely written out
		// before we write the next level.
		if err := e.Events.Flush(ctx); err != nil {
			return errors.Wrap(err, "orderedEvents flush")
		}
		for _, def := range defs {
			if err := e.Events.OnData(ctx, def.source, def.target, def.muts); err != nil {
				return errors.Wrap(err, "orderedEvents OnData")
			}
		}
	}
	return errors.Wrap(e.Events.OnCommit(ctx), "orderedEvents OnCommit")
}

// OnData implements Events. Updates to root tables will pass through
// immediately.  Other updates will be assigned to their dependency
// level, to be flushed by OnCommit.
func (e *orderedEvents) OnData(
	ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	destLevel, ok := e.levels[target]
	if !ok {
		return errors.Errorf("unknown destination table %s", target)
	}
	if destLevel == 0 {
		return errors.Wrap(e.Events.OnData(ctx, source, target, muts), "orderedEvents OnData")
	}
	e.deferred[destLevel-1] = append(e.deferred[destLevel-1], deferredData{muts, source, target})
	return nil
}

// OnRollback implements Events. It resets the internal state.
func (e *orderedEvents) OnRollback(ctx context.Context, msg Message) error {
	e.reset()
	return e.Events.OnRollback(ctx, msg)
}

// reset cleans up the internal state, ready for a call to OnBegin.
func (e *orderedEvents) reset() {
	for idx, defs := range e.deferred {
		e.deferred[idx] = defs[:0:batches.Size()]
	}
}
