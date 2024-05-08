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

package stage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/sinktest/mutations"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/notify"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutAndDrain will insert and mark a batch of Mutations.
func TestPutAndDrain(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	a.NotEmpty(fixture.StagingPool.Version)
	pool := fixture.StagingPool
	targetDB := fixture.StagingDB.Schema()

	dummyTarget := ident.NewTable(targetDB, ident.New("target"))

	s, err := fixture.Stagers.Get(ctx, dummyTarget)
	r.NoError(err)
	a.NotNil(s)

	// Not part of public API, but we want to test the metrics function.
	ctr := s.(interface {
		CountUnapplied(
			ctx context.Context, db types.StagingQuerier, before hlc.Time, aost bool,
		) (int, error)
	})

	jumbledStager, err := fixture.Stagers.Get(ctx, sinktest.JumbleTable(dummyTarget))
	r.NoError(err)
	a.Same(s, jumbledStager)

	// Steal implementation details to cross-check DB state.
	stagingTable := s.(interface{ GetTable() ident.Table }).GetTable()

	// Cook test data.
	const total = 10_000
	muts := make([]types.Mutation, total)
	for i := range muts {
		muts[i] = types.Mutation{
			Data: []byte(fmt.Sprintf(`{"pk": %d}`, i)),
			Key:  []byte(fmt.Sprintf(`[%d]`, i)),
			Time: hlc.New(int64(1000*i)+2, i),
		}
		// Don't assume that all mutations have a Before value.
		if i%10 == 0 {
			muts[i].Before = []byte("before")
		}
	}
	endTime := muts[len(muts)-1].Time

	// Insert.
	r.NoError(s.Stage(ctx, pool, muts))

	// Sanity-check table.
	count, err := base.GetRowCount(ctx, pool, stagingTable)
	r.NoError(err)
	a.Equal(total, count)

	// Ensure that data insertion is idempotent.
	r.NoError(s.Stage(ctx, pool, muts))

	// Sanity-check table.
	count, err = base.GetRowCount(ctx, pool, stagingTable)
	r.NoError(err)
	a.Equal(total, count)

	// Verify metrics query. We can't test AOST=true since the schema
	// change will not have taken place yet.
	count, err = ctr.CountUnapplied(ctx, pool, endTime.Next(), false)
	r.NoError(err)
	a.Equal(total, count)

	// Mark all mutations with the applied flag. This allows the
	// mutations to be deleted.
	r.NoError(s.MarkApplied(ctx, pool, muts))

	// Verify metrics query.
	count, err = ctr.CountUnapplied(ctx, pool, endTime.Next(), false)
	r.NoError(err)
	a.Zero(count)

	// Retire mutations.
	r.NoError(s.Retire(ctx, pool, endTime))

	// Should be empty now.
	count, err = base.GetRowCount(ctx, pool, stagingTable)
	r.NoError(err)
	a.Equal(0, count)

	// Verify various no-op calls are OK.
	r.NoError(s.Retire(ctx, pool, hlc.Zero()))
	r.NoError(s.Retire(ctx, pool, endTime))
	r.NoError(s.Retire(ctx, pool, endTime.Next()))
}

func TestStoreIfExists(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	pool := fixture.StagingPool
	targetDB := fixture.StagingDB.Schema()

	bounds := hlc.RangeIncluding(hlc.Zero(), hlc.New(100, 0))
	dummyTarget := ident.NewTable(targetDB, ident.New("target"))

	s, err := fixture.Stagers.Get(ctx, dummyTarget)
	r.NoError(err)

	toMark := []types.Mutation{
		{
			Data: json.RawMessage(`{pk:1}`),
			Key:  json.RawMessage(`[1]`),
			Time: hlc.New(1, 0),
		},
	}
	// Store a seed mutation.
	r.NoError(s.Stage(ctx, pool, toMark))

	found, err := fixture.PeekStaged(ctx, dummyTarget, bounds)
	r.NoError(err)
	r.Len(found, 1)

	proposed := []types.Mutation{
		// New entry, should not be staged.
		{
			Data: json.RawMessage(`{pk:0}`),
			Key:  json.RawMessage(`[0]`),
			Time: hlc.New(2, 0),
		},
		// This should be staged.
		{
			Data: json.RawMessage(`{pk:1}`),
			Key:  json.RawMessage(`[1]`),
			Time: hlc.New(2, 0),
		},
		// New entry, should not be staged.
		{
			Data: json.RawMessage(`{pk:2}`),
			Key:  json.RawMessage(`[2]`),
			Time: hlc.New(2, 0),
		},
	}
	toMark = append(toMark, proposed...)

	pending, err := s.StageIfExists(ctx, pool, proposed)
	r.NoError(err)
	r.Equal([]types.Mutation{proposed[0], proposed[2]}, pending)

	found, err = fixture.PeekStaged(ctx, dummyTarget, bounds)
	r.NoError(err)
	r.Len(found, 2)

	// Bump the timestamp and try again. We should see the same
	// keys be selected.
	for idx := range proposed {
		proposed[idx].Time = hlc.New(3, 0)
	}
	toMark = append(toMark, proposed...)

	pending, err = s.StageIfExists(ctx, pool, proposed)
	r.NoError(err)
	r.Equal([]types.Mutation{proposed[0], proposed[2]}, pending)

	found, err = fixture.PeekStaged(ctx, dummyTarget, bounds)
	r.NoError(err)
	r.Len(found, 3)

	// Mark the mutations.
	r.NoError(s.MarkApplied(ctx, pool, toMark))

	found, err = fixture.PeekStaged(ctx, dummyTarget, bounds)
	r.NoError(err)
	r.Empty(found)

	// Since the staging table is effectively empty, this call should be
	// a no-op.
	pending, err = s.StageIfExists(ctx, pool, proposed)
	r.NoError(err)
	r.Equal(proposed, pending)
}

func TestRead(t *testing.T) {
	const entries = 1_000 // Increase for ersatz perf testing.
	const tableCount = 10
	a := assert.New(t)
	r := require.New(t)
	start := time.Now()
	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context
	a.NotEmpty(fixture.StagingPool.Version)

	// Create some fake table names.
	targetDB := fixture.TargetSchema.Schema()
	tables := make([]ident.Table, tableCount)
	for idx := range tables {
		tables[idx] = ident.NewTable(targetDB, ident.New(fmt.Sprintf("target_%d", idx)))
	}
	// Set up table groupings, to simulate FK use-cases.
	tableGroups := [][]ident.Table{
		{tables[0]},
		{tables[1], tables[2]},
		{tables[3], tables[4], tables[5]},
		{tables[6], tables[7], tables[8], tables[9]},
	}
	tableToGroup := &ident.TableMap[int]{}
	for group, tables := range tableGroups {
		for _, table := range tables {
			tableToGroup.Put(table, group)
		}
	}

	// Each table will have the following data inserted:
	// * Large batch of entries at t=1
	// * Individual entries from t=[2*entries, 3*entries]
	// * Large batch at t=10*entries
	// * Individual entries at t=[12*entries, 13*entries]
	muts := make([]types.Mutation, 0, 4*entries)
	for i := 0; i < entries; i++ {
		muts = append(muts,
			// Batch at t=1
			types.Mutation{
				Before: []byte("null"),
				Data:   []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:    []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time:   hlc.New(1, 0),
			},
			// Individual with varying timestamps
			types.Mutation{
				Before: []byte(`{"pk":%d}`),
				Data:   []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:    []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time:   hlc.New(int64(2*entries+i), 0),
			},
			// Batch at t=10*entries
			types.Mutation{
				Before: []byte(`{"pk":%d}`),
				Data:   []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:    []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time:   hlc.New(10*entries, 0),
			},
			// More individual entries with varying timestamps
			types.Mutation{
				Before: []byte(`{"pk":%d}`),
				Data:   []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:    []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time:   hlc.New(int64(12*entries+i), 0),
			})
	}
	r.Len(muts, 4*entries)

	// Order by time, then lexicographically by key.
	expectedMutOrder := append([]types.Mutation(nil), muts...)
	sort.Slice(expectedMutOrder, func(i, j int) bool {
		iMut := expectedMutOrder[i]
		jMut := expectedMutOrder[j]
		if c := hlc.Compare(iMut.Time, jMut.Time); c != 0 {
			return c < 0
		}
		return bytes.Compare(expectedMutOrder[i].Key, expectedMutOrder[j].Key) < 0
	})

	// Stage some data for each table.
	stagingTables := &ident.TableMap[ident.Table]{}
	for _, table := range tables {
		stager, err := fixture.Stagers.Get(ctx, table)
		r.NoError(err)
		r.NoError(stager.Stage(ctx, fixture.StagingPool, muts))
		stagingTables.Put(table, stager.(interface{ GetTable() ident.Table }).GetTable())
	}

	log.Infof("finished filling data in %s", time.Since(start))

	// Read all data back. We'll use a small-ish segment length to ensure
	// that pagination is correct.
	bounds := hlc.RangeIncluding(hlc.Zero(), hlc.New(20*entries, 0))
	batch, err := fixture.ReadStagingQuery(ctx, &types.StagingQuery{
		Bounds:       notify.VarOf(bounds),
		FragmentSize: entries / 100,
		Group: &types.TableGroup{
			Enclosing: fixture.TargetSchema.Schema(),
			Name:      ident.New("testing"),
			Tables:    tables,
		},
	})
	r.NoError(err)
	a.Equal(4*entries*tableCount, batch.Count())

	// Read each table's data back.
	for _, table := range tables {
		muts, err := fixture.PeekStaged(ctx, table, bounds)
		r.NoError(err)
		a.Equal(expectedMutOrder, muts)
	}
}

func BenchmarkStage(b *testing.B) {
	sizes := []int{
		1,
		10,
		100,
		1000,
		10000,
	}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			benchmarkStage(b, size)
		})
	}
}

func benchmarkStage(b *testing.B, batchSize int) {
	fixture, err := all.NewFixture(b)
	if err != nil {
		b.Fatal(err)
	}

	ctx := fixture.Context
	targetDB := fixture.TargetSchema.Schema()

	dummyTarget := ident.NewTable(targetDB, ident.New("target"))

	s, err := fixture.Stagers.Get(ctx, dummyTarget)
	if err != nil {
		b.Fatal(err)
	}

	var allBytes atomic.Int64
	muts := mutations.Generator(ctx, 100000, 0.5)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		batch := make([]types.Mutation, batchSize)
		for pb.Next() {
			for i := range batch {
				mut := <-muts
				batch[i] = mut
				allBytes.Add(int64(len(mut.Data) + len(mut.Key)))
			}
			if err := s.Stage(ctx, fixture.StagingPool, batch); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Use JSON byte count as throughput measure.
	b.SetBytes(allBytes.Load())

}
