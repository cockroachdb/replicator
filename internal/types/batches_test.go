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
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/mutations"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/require"
)

var (
	fakeTable = ident.NewTable(ident.MustSchema(ident.New("schema")), ident.New("fake_table"))
	fakeTime  = hlc.New(100, 100)
)

func TestMultiBatch(t *testing.T) {
	batch := &types.MultiBatch{}
	testBatchInterface(t, batch)
}

func TestTableBatch(t *testing.T) {
	batch := &types.TableBatch{Table: fakeTable, Time: fakeTime}
	testBatchInterface(t, batch)
}

func TestTemporalBatch(t *testing.T) {
	batch := &types.TemporalBatch{Time: fakeTime}
	testBatchInterface(t, batch)
}

func testBatchInterface[B types.Batch[B]](t *testing.T, batch B) {
	t.Helper()
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.Equal(0, batch.Count())

	const mutCount = 100
	data := mutations.Generator(ctx, 1024 /* distinct ids */, 0.1)
	expected := make([]types.Mutation, mutCount)
	for i := 0; i < mutCount; i++ {
		mut := <-data
		// We want the multi-batch sorted by time.
		if _, isMulti := any(batch).(*types.MultiBatch); isMulti {
			mut.Time = hlc.New(int64(i+1), i+1)
		} else {
			mut.Time = fakeTime
		}
		expected[i] = mut
		r.NoError(batch.Accumulate(fakeTable, mut))
	}
	r.Equal(mutCount, batch.Count())

	// These types wind up being sorted by key.
	switch any(batch).(type) {
	case *types.TableBatch, *types.TemporalBatch:
		sort.Slice(expected, func(i, j int) bool {
			return bytes.Compare(expected[i].Key, expected[j].Key) < 0
		})
	}

	cpy := batch.Copy()
	r.NotSame(batch, cpy)
	r.Equal(batch.Count(), cpy.Count())

	r.Zero(batch.Empty().Count())

	acc := &types.MultiBatch{}
	r.NoError(batch.CopyInto(acc))
	r.Equal(batch.Count(), acc.Count())

	flattened := types.Flatten[B](batch)
	r.Equal(expected, flattened)
}
