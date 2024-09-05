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

package types_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/recorder"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/workload"
	"github.com/stretchr/testify/require"
)

func TestOrderedAcceptor(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build fake schema data showing tables spread across a number of
	// dependency levels.
	const levels = 3
	const tables = 3
	allTables := make([]ident.Table, 0, levels*tables)
	schema := ident.MustSchema(ident.New("my_db"), ident.New("public"))
	schemaData := &types.SchemaData{
		Entire: &types.SchemaComponent{},
	}
	for levelIdx := range levels {
		for tableIdx := range tables {
			table := ident.NewTable(schema, ident.New(fmt.Sprintf("%d-%d", levelIdx, tableIdx)))
			schemaData.Entire.Order = append(schemaData.Entire.Order, table)
			allTables = append(allTables, table)
		}
	}
	schemaData.Entire.ReverseOrder = slices.Clone(schemaData.Entire.Order)
	slices.Reverse(schemaData.Entire.ReverseOrder)

	// Create a batch with mutations striped across the levels.
	const mutations = 128
	batch := &types.MultiBatch{}
	for i := 0; i < mutations; i++ {
		r.NoError(batch.Accumulate(allTables[i%len(allTables)], types.Mutation{
			Data: json.RawMessage(`{"fake":true}`),
			Key:  json.RawMessage(`[ "fake" ]`),
			Time: hlc.New(int64(i+1), i+1),
		}))
	}

	rec := &recorder.Recorder{}
	acc := types.OrderedAcceptorFrom(rec, &fakeWatchers{schemaData})
	// We check that an empty batch works
	r.NoError(acc.AcceptMultiBatch(ctx, &types.MultiBatch{}, &types.AcceptOptions{}))
	// We check that the batch created before works
	r.NoError(acc.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{}))

	// Validate unwrapping.
	r.Same(acc, types.OrderedAcceptorFrom(acc, &fakeWatchers{schemaData}))
	r.Same(acc, types.UnorderedAcceptorFrom(acc))

	// We expect to see exactly one call per table and the table order
	// should respect the level order.
	calls := rec.Calls()
	r.Len(calls, len(allTables))
	expectedLevelIdx := 0
	for _, call := range calls {
		tableBatch := call.Table
		r.NotNil(tableBatch)

		// There's no specific order for tables within a level.
		var levelIdx, tableIdx int
		_, err := fmt.Sscanf(tableBatch.Table.Table().Raw(), "%d-%d", &levelIdx, &tableIdx)
		r.NoError(err, tableBatch.Table.Table().Raw())

		if levelIdx == expectedLevelIdx {
			// OK
		} else if levelIdx == expectedLevelIdx+1 {
			// This should be a ratchet.
			expectedLevelIdx = levelIdx
		} else {
			r.Failf("unexpected state", "levelIdx=%d, expectedLevelIdx=%d",
				levelIdx, expectedLevelIdx)
		}
	}
	r.Equal(levels-1, expectedLevelIdx)

	// Detect cases where an unknown table is present in the input.
	unknownTable := ident.NewTable(schema, ident.New("unknown"))
	badBatch := &types.MultiBatch{}
	r.NoError(badBatch.Accumulate(unknownTable, types.Mutation{
		Deletion: true,
		Time:     hlc.New(1, 1),
	}))
	r.ErrorContains(
		acc.AcceptMultiBatch(ctx, badBatch, &types.AcceptOptions{}),
		"delete sent to unknown table")
}

func TestUnorderedAcceptor(t *testing.T) {
	const batches = 1000
	r := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := ident.MustSchema(ident.New("my_db"), ident.New("public"))

	gen := workload.NewGeneratorBase(
		ident.NewTable(schema, ident.New("parent")),
		ident.NewTable(schema, ident.New("child")))
	batch := &types.MultiBatch{}
	for i := range batches {
		gen.GenerateInto(batch, hlc.New(int64(i+1), i))
	}

	rec := &recorder.Recorder{}
	acc := types.UnorderedAcceptorFrom(rec)
	r.NoError(acc.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{}))
	r.Equal(batch.Count(), rec.Count())
	for _, call := range rec.Calls() {
		r.NotNil(call.Table)
		r.Nil(call.Multi)
		r.Nil(call.Temporal)
	}

	// Validate unwrapping.
	r.NotSame(acc, types.OrderedAcceptorFrom(acc, &fakeWatchers{}))
	r.Same(acc, types.UnorderedAcceptorFrom(acc))
}

type fakeWatchers struct {
	data *types.SchemaData
}

func (w *fakeWatchers) Get(sch ident.Schema) (types.Watcher, error) {
	if sch.Empty() {
		return nil, errors.New("empty schema")
	}
	return &fakeWatcher{w.data}, nil
}

type fakeWatcher struct {
	data *types.SchemaData
}

func (w *fakeWatcher) Get() *types.SchemaData { return w.data }

func (w *fakeWatcher) GetNotify() *notify.Var[*types.SchemaData] { return notify.VarOf(w.data) }

func (w *fakeWatcher) Refresh(_ context.Context, _ *types.TargetPool) error {
	return errors.New("unimplemented")
}
func (w *fakeWatcher) Watch(
	_ *stopper.Context, _ ident.Table,
) (*notify.Var[[]types.ColData], error) {
	return nil, errors.New("unimplemented")
}
