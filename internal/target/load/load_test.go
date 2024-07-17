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

package load_test

import (
	"testing"

	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/target/load"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/crep"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/merge"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	work, _, err := fixture.NewWorkload(ctx, &all.WorkloadConfig{})
	r.NoError(err)

	// Generate some mutations and apply them directly to the target.
	batch := &types.MultiBatch{}
	for i := range 100 {
		work.GenerateInto(batch, hlc.New(int64(i+1), i))
	}
	acc := types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers)
	r.NoError(acc.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{}))

	// Double-check the data exists.
	r.True(work.CheckConsistent(ctx, t))

	parentCols, ok := fixture.Watcher.Get().Columns.Get(work.Parent.Name())
	r.True(ok)
	childCols, ok := fixture.Watcher.Get().Columns.Get(work.Child.Name())
	r.True(ok)

	childCol := ident.New("child")
	parentCol := ident.New("parent")
	valCol := ident.New("val")

	// Summarize all parent rows.
	parentRows := work.ParentRows()
	parentsToFill := make([]*merge.Bag, len(parentRows))
	for idx, row := range parentRows {
		parentsToFill[idx] = merge.NewBagOf(parentCols, nil, parentCol, row.ID)
	}

	// Summarize all child rows.
	childRows := work.ChildRows()
	childrenToFill := make([]*merge.Bag, len(childRows))
	for idx, row := range childRows {
		childrenToFill[idx] = merge.NewBagOf(childCols, nil, childCol, row.ID)
	}

	// See comment in ProvideLoader.
	if fixture.TargetPool.Product == types.ProductMariaDB {
		_, err := fixture.Loader.Load(ctx, fixture.TargetPool, work.Parent.Name(), parentsToFill)
		r.ErrorIs(err, load.ErrUnimplemented)
		return
	}

	// Load all columns.
	t.Run("smoke", func(t *testing.T) {
		r := require.New(t)

		result, err := fixture.Loader.Load(ctx, fixture.TargetPool, work.Parent.Name(), parentsToFill)
		r.NoError(err)
		r.Equal(parentsToFill, result.Dirty)
		r.Empty(result.NotFound)
		r.Empty(result.Unmodified)

		result, err = fixture.Loader.Load(ctx, fixture.TargetPool, work.Child.Name(), childrenToFill)
		r.NoError(err)
		r.Equal(childrenToFill, result.Dirty)
		r.Empty(result.NotFound)
		r.Empty(result.Unmodified)

		// Verify data against workload generator.
		for _, filled := range parentsToFill {
			id := filled.GetZero(parentCol).(int)
			val := filled.GetZero(valCol)

			expectedVal, ok := work.ParentVals[id]
			r.True(ok)
			r.True(crep.Equal(expectedVal, val))
		}

		for _, filled := range childrenToFill {
			id := filled.GetZero(childCol).(int)
			parent := filled.GetZero(parentCol)
			val := filled.GetZero(valCol)

			expectedParent, ok := work.ChildToParent[id]
			r.True(ok)
			r.True(crep.Equal(expectedParent, parent))

			expectedVals, ok := work.ChildVals[id]
			r.True(ok)
			r.True(crep.Equal(expectedVals, val))
		}
	})

	// Verify that re-load is a no-op.
	t.Run("no_op", func(t *testing.T) {
		r := require.New(t)
		result, err := fixture.Loader.Load(ctx,
			fixture.TargetPool, work.Parent.Name(), parentsToFill)
		r.NoError(err)
		r.Empty(result.Dirty)
		r.Empty(result.NotFound)
		r.Equal(parentsToFill, result.Unmodified)
	})

	// Test API for keys that don't match.
	t.Run("no_match", func(t *testing.T) {
		r := require.New(t)
		doesNotExist := merge.NewBagOf(parentCols, nil, parentCol, -42)
		result, err := fixture.Loader.Load(ctx,
			fixture.TargetPool, work.Parent.Name(), []*merge.Bag{doesNotExist})
		r.NoError(err)
		r.Empty(result.Dirty)
		r.Equal([]*merge.Bag{doesNotExist}, result.NotFound)
		r.Empty(result.Unmodified)

		_, ok := doesNotExist.Get(valCol)
		r.False(ok)
	})

	// Check sparse data columns.
	t.Run("partial_load", func(t *testing.T) {
		r := require.New(t)

		id := childRows[0].ID
		expectedVal := work.ChildVals[id]

		sparse := merge.NewBagOf(childCols, nil,
			childCol, id,
			parentCol, -42, // This shouldn't be overwritten.
		)
		result, err := fixture.Loader.Load(ctx,
			fixture.TargetPool, work.Child.Name(), []*merge.Bag{sparse})
		r.NoError(err)
		r.Equal([]*merge.Bag{sparse}, result.Dirty)
		r.Empty(result.NotFound)
		r.Empty(result.Unmodified)

		r.Equal(-42, sparse.GetZero(parentCol))
		r.True(crep.Equal(expectedVal, sparse.GetZero(valCol)))
	})
}
