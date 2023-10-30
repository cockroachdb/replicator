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
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/mutations"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutAndDrain will insert and dequeue a batch of Mutations.
func TestPutAndDrain(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, cancel, err := all.NewFixture()
	r.NoError(err)
	defer cancel()

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
		CountUnapplied(ctx context.Context, db types.StagingQuerier, before hlc.Time) (int, error)
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

	// Insert.
	r.NoError(s.Store(ctx, pool, muts))

	// Sanity-check table.
	count, err := base.GetRowCount(ctx, pool, stagingTable)
	r.NoError(err)
	a.Equal(total, count)

	// Ensure that data insertion is idempotent.
	r.NoError(s.Store(ctx, pool, muts))

	// Sanity-check table.
	count, err = base.GetRowCount(ctx, pool, stagingTable)
	r.NoError(err)
	a.Equal(total, count)

	// Verify metrics query.
	count, err = ctr.CountUnapplied(ctx, pool, hlc.New(math.MaxInt64, 0))
	r.NoError(err)
	a.Equal(total, count)

	// Select all mutations to set the applied flag. This allows the
	// mutations to be deleted.
	cursor := &types.UnstageCursor{
		EndBefore: hlc.New(math.MaxInt64, 0),
		Targets:   []ident.Table{dummyTarget},
	}
	unstagedCount := 0
	for unstaging := true; unstaging; {
		cursor, unstaging, err = fixture.Stagers.Unstage(ctx, pool, cursor,
			func(context.Context, ident.Table, types.Mutation) error {
				unstagedCount++
				return nil
			})
		r.NoError(err)
	}
	a.Equal(total, unstagedCount)

	// Verify metrics query.
	count, err = ctr.CountUnapplied(ctx, pool, hlc.New(math.MaxInt64, 0))
	r.NoError(err)
	a.Zero(count)

	// Retire mutations.
	r.NoError(s.Retire(ctx, pool, muts[len(muts)-1].Time))

	// Should be empty now.
	count, err = base.GetRowCount(ctx, pool, stagingTable)
	r.NoError(err)
	a.Equal(0, count)

	// Verify various no-op calls are OK.
	r.NoError(s.Retire(ctx, pool, hlc.Zero()))
	r.NoError(s.Retire(ctx, pool, muts[len(muts)-1].Time))
	r.NoError(s.Retire(ctx, pool, hlc.New(muts[len(muts)-1].Time.Nanos()+1, 0)))
}

func TestUnstage(t *testing.T) {
	const entries = 100
	const tableCount = 10
	a := assert.New(t)
	r := require.New(t)

	fixture, cancel, err := all.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

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
	for _, table := range tables {
		stager, err := fixture.Stagers.Get(ctx, table)
		r.NoError(err)
		r.NoError(stager.Store(ctx, fixture.StagingPool, muts))
	}

	t.Run("transactional", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.UnstageCursor{
			EndBefore: hlc.New(100*entries, 0), // Past any existing time.
			Targets:   tables,
		}

		// Run the select in a discarded transaction to avoid
		// contaminating future tests with side effects.
		tx, err := fixture.StagingPool.BeginTx(ctx, pgx.TxOptions{})
		r.NoError(err)
		defer func() { _ = tx.Rollback(ctx) }()

		entriesByTable := &ident.TableMap[[]types.Mutation]{}
		numSelections := 0
		for selecting := true; selecting; {
			q, selecting, err = fixture.Stagers.Unstage(ctx, tx, q,
				func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
					entriesByTable.Put(tbl, append(entriesByTable.GetZero(tbl), mut))
					return nil
				})
			r.NoError(err)
			numSelections++
		}
		// Refer to comment about the distribution of timestamps.
		// There are two large batches, then each mutation has two
		// unique timestamps, and then there's a final call that returns
		// false.
		a.Equal(2+2*entries+1, numSelections)

		r.NoError(entriesByTable.Range(func(_ ident.Table, seen []types.Mutation) error {
			if a.Len(seen, len(expectedMutOrder)) {
				a.Equal(expectedMutOrder, seen)
			}
			return nil
		}))
	})

	// Read the middle two tranches of updates.
	t.Run("transactional-bounded", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.UnstageCursor{
			StartAt:   hlc.New(2, 0),          // Skip the initial transaction
			EndBefore: hlc.New(10*entries, 1), // Read the second large batch
			Targets:   tables,
		}

		// Run the select in a discarded transaction to avoid
		// contaminating future tests with side effects.
		tx, err := fixture.StagingPool.BeginTx(ctx, pgx.TxOptions{})
		r.NoError(err)
		defer func() { _ = tx.Rollback(ctx) }()

		entriesByTable := &ident.TableMap[[]types.Mutation]{}
		numSelections := 0
		for selecting := true; selecting; {
			q, selecting, err = fixture.Stagers.Unstage(ctx, tx, q,
				func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
					entriesByTable.Put(tbl, append(entriesByTable.GetZero(tbl), mut))
					return nil
				})
			r.NoError(err)
			numSelections++
		}
		// We expect to see one large batch, a timestamp for each entry,
		// and the final zero-results call.
		a.Equal(1+entries+1, numSelections)
		r.NoError(entriesByTable.Range(func(_ ident.Table, seen []types.Mutation) error {
			if a.Len(seen, 2*entries) {
				a.Equal(expectedMutOrder[entries:3*entries], seen)
			}
			return nil
		}))
	})

	// Read from the staging tables using the limit, to simulate
	// very large batches.
	t.Run("transactional-incremental", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.UnstageCursor{
			EndBefore:   hlc.New(100*entries, 0), // Past any existing time.
			Targets:     tables,
			UpdateLimit: 20,
		}

		// Run the select in a discarded transaction to avoid
		// contaminating future tests with side effects.
		tx, err := fixture.StagingPool.BeginTx(ctx, pgx.TxOptions{})
		r.NoError(err)
		defer func() { _ = tx.Rollback(ctx) }()

		entriesByTable := &ident.TableMap[[]types.Mutation]{}
		numSelections := 0
		for selecting := true; selecting; {
			q, selecting, err = fixture.Stagers.Unstage(ctx, tx, q,
				func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
					entriesByTable.Put(tbl, append(entriesByTable.GetZero(tbl), mut))
					return nil
				})
			r.NoError(err)
			numSelections++
		}
		a.Equal(211, numSelections)
		r.NoError(entriesByTable.Range(func(_ ident.Table, seen []types.Mutation) error {
			if a.Len(seen, len(expectedMutOrder)) {
				a.Equal(expectedMutOrder, seen)
			}
			return nil
		}))
	})
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
	fixture, cancel, err := all.NewFixture()
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()

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
			if err := s.Store(ctx, fixture.StagingPool, batch); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Use JSON byte count as throughput measure.
	b.SetBytes(allBytes.Load())

}
