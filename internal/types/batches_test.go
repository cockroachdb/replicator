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

	now := hlc.New(100, 100)
	r.Equal(0, batch.Count())

	const mutCount = 100
	for mut := range mutations.Generator(ctx, 1024 /* distinct ids */, 0.1) {
		mut.Time = now
		r.NoError(batch.Accumulate(fakeTable, mut))
		if batch.Count() == mutCount {
			break
		}
	}
	r.Equal(mutCount, batch.Count())

	cpy := batch.Copy()
	r.NotSame(batch, cpy)
	r.Equal(batch.Count(), cpy.Count())

	r.Zero(batch.Empty().Count())
}
