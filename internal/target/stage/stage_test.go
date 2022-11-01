// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stage_test

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutAndDrain will insert and dequeue a batch of Mutations.
func TestPutAndDrain(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	a.NotEmpty(fixture.DBInfo.Version())

	targetDB := fixture.TestDB.Ident()

	dummyTarget := ident.NewTable(
		targetDB, ident.Public, ident.New("target"))

	s, err := fixture.Stagers.Get(ctx, dummyTarget)
	if !a.NoError(err) {
		return
	}
	a.NotNil(s)

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
	}
	maxTime := muts[len(muts)-1].Time

	// Check TransactionTimes in empty state.
	found, err := s.TransactionTimes(ctx, fixture.Pool, hlc.Zero(), maxTime)
	a.Empty(found)
	a.NoError(err)

	// Insert.
	a.NoError(s.Store(ctx, fixture.Pool, muts))

	// We should find all timestamps now.
	found, err = s.TransactionTimes(ctx, fixture.Pool, hlc.Zero(), maxTime)
	if a.Len(found, len(muts)) {
		a.Equal(muts[0].Time, found[0])
		a.Equal(maxTime, found[len(found)-1])
	}
	a.NoError(err)

	// Advance once.
	found, err = s.TransactionTimes(ctx, fixture.Pool, muts[0].Time, maxTime)
	if a.Len(found, len(muts)-1) {
		a.Equal(muts[1].Time, found[0])
		a.Equal(maxTime, found[len(found)-1])
	}
	a.NoError(err)

	// Make sure we don't find the last value.
	found, err = s.TransactionTimes(ctx, fixture.Pool, maxTime, maxTime)
	a.Empty(found)
	a.NoError(err)

	// Sanity-check table.
	count, err := sinktest.GetRowCount(ctx, fixture.Pool, stagingTable)
	a.NoError(err)
	a.Equal(total, count)

	// Ensure that data insertion is idempotent.
	a.NoError(s.Store(ctx, fixture.Pool, muts))

	// Sanity-check table.
	count, err = sinktest.GetRowCount(ctx, fixture.Pool, stagingTable)
	a.NoError(err)
	a.Equal(total, count)

	// Insert an older value for each key, we'll check that only the
	// latest values are returned below.
	older := make([]types.Mutation, total)
	copy(older, muts)
	for i := range older {
		older[i].Data = []byte(`"should not see this"`)
		older[i].Time = hlc.New(older[i].Time.Nanos()-1, i)
	}
	a.NoError(s.Store(ctx, fixture.Pool, older))

	// Sanity-check table.
	count, err = sinktest.GetRowCount(ctx, fixture.Pool, stagingTable)
	a.NoError(err)
	a.Equal(2*total, count)

	// The two queries that we're going to run will see slightly
	// different views of the data. The all-data query will provide
	// deduplicated data, ordered by the target key. The incremental
	// query will see all values interleaved, which allows us to
	// page within a (potentially large) backfill window.
	dedupOrder := make([]types.Mutation, total)
	copy(dedupOrder, muts)
	sort.Slice(dedupOrder, func(i, j int) bool {
		return bytes.Compare(dedupOrder[i].Key, dedupOrder[j].Key) < 0
	})
	mergedOrder := make([]types.Mutation, 2*total)
	for i := range muts {
		mergedOrder[2*i] = older[i]
		mergedOrder[2*i+1] = muts[i]
	}

	// Test retrieving all data.
	ret, err := s.Select(ctx, fixture.Pool, hlc.Zero(), hlc.New(int64(1000*total+1), 0))
	a.NoError(err)
	a.Equal(mergedOrder, ret)

	// Retrieve a few pages of partial values, validate expected boundaries.
	const limit = 10
	var tail types.Mutation
	for i := 0; i < 10; i++ {
		ret, err = s.SelectPartial(ctx,
			fixture.Pool,
			tail.Time,
			muts[len(muts)-1].Time,
			tail.Key,
			limit,
		)
		a.NoError(err)
		a.Equalf(mergedOrder[i*limit:(i+1)*limit], ret, "at idx %d", i)
		tail = ret[len(ret)-1]
	}
	a.NoError(s.Retire(ctx, fixture.Pool, muts[limit-1].Time))

	// Verify that reading from the end returns no results.
	ret, err = s.SelectPartial(ctx,
		fixture.Pool,
		muts[len(muts)-1].Time,
		hlc.New(math.MaxInt64, 0),
		muts[len(muts)-1].Key,
		limit,
	)
	a.NoError(err)
	a.Empty(ret)

	// Check deletion. We have two timestamps for each
	count, err = sinktest.GetRowCount(ctx, fixture.Pool, stagingTable)
	a.NoError(err)
	a.Equal(2*total-2*limit, count)

	// Dequeue remainder.
	a.NoError(s.Retire(ctx, fixture.Pool, muts[len(muts)-1].Time))

	// Should be empty now.
	count, err = sinktest.GetRowCount(ctx, fixture.Pool, stagingTable)
	a.NoError(err)
	a.Equal(0, count)

	// Verify various no-op calls are OK.
	a.NoError(s.Retire(ctx, fixture.Pool, hlc.Zero()))
	a.NoError(s.Retire(ctx, fixture.Pool, muts[len(muts)-1].Time))
	a.NoError(s.Retire(ctx, fixture.Pool, hlc.New(muts[len(muts)-1].Time.Nanos()+1, 0)))
}

func TestSelectMany(t *testing.T) {
	const entries = 100
	const tableCount = 10
	a := assert.New(t)
	r := require.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	a.NotEmpty(fixture.DBInfo.Version())

	// Create some fake table names.
	targetDB := fixture.TestDB.Ident()
	tables := make([]ident.Table, tableCount)
	for idx := range tables {
		tables[idx] = ident.NewTable(
			targetDB, ident.Public, ident.New(fmt.Sprintf("target_%d", idx)))
	}
	// Set up table groupings, to simulate FK use-cases.
	tableGroups := [][]ident.Table{
		{tables[0]},
		{tables[1], tables[2]},
		{tables[3], tables[4], tables[5]},
		{tables[6], tables[7], tables[8], tables[9]},
	}
	tableToGroup := make(map[ident.Table]int)
	for group, tables := range tableGroups {
		for _, table := range tables {
			tableToGroup[table] = group
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
				Data: []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:  []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time: hlc.New(1, 0),
			},
			// Individual with varying timestamps
			types.Mutation{
				Data: []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:  []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time: hlc.New(int64(2*entries+i), 0),
			},
			// Batch at t=10*entries
			types.Mutation{
				Data: []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:  []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time: hlc.New(10*entries, 0),
			},
			// More individual entries with varying timestamps
			types.Mutation{
				Data: []byte(fmt.Sprintf(`{"pk":%d}`, i)),
				Key:  []byte(fmt.Sprintf(`[ %d ]`, i)),
				Time: hlc.New(int64(12*entries+i), 0),
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
		r.NoError(stager.Store(ctx, fixture.Pool, muts))
	}

	t.Run("transactional", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.SelectManyCursor{
			Targets: [][]ident.Table{
				{tables[0]},
				{tables[1], tables[2]},
				{tables[3], tables[4], tables[5]},
				{tables[6], tables[7], tables[8], tables[9]},
			},
			Limit: entries/2 - 1,           // Validate paging
			Until: hlc.New(100*entries, 0), // Past any existing time.
		}

		entriesByTable := make(map[ident.Table][]types.Mutation)
		for {
			data, more, err := fixture.Stagers.SelectMany(ctx, fixture.Pool, q)
			r.NoError(err)
			for tbl, tblMuts := range data {
				entriesByTable[tbl] = append(entriesByTable[tbl], tblMuts...)
			}
			if !more {
				break
			}
		}
		for _, seen := range entriesByTable {
			if a.Len(seen, len(expectedMutOrder)) {
				a.Equal(expectedMutOrder, seen)
			}
		}
	})

	// What's different about the backfill case is that we want to
	// verify that if we see an update for a table in group G, then we
	// already have all data for group G-1.
	t.Run("backfill", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.SelectManyCursor{
			Backfill: true,
			Targets: [][]ident.Table{
				{tables[0]},
				{tables[1], tables[2]},
				{tables[3], tables[4], tables[5]},
				{tables[6], tables[7], tables[8], tables[9]},
			},
			Limit: entries/2 - 1,           // Validate paging
			Until: hlc.New(100*entries, 0), // Past any existing time.
		}

		entriesByTable := make(map[ident.Table][]types.Mutation)
		for {
			data, more, err := fixture.Stagers.SelectMany(ctx, fixture.Pool, q)
			r.NoError(err)
			for tbl, tblMuts := range data {
				entriesByTable[tbl] = append(entriesByTable[tbl], tblMuts...)
			}

			// Check previous groups by induction. We do this in a
			// separate loop, since we may cross a group boundary within
			// a page of data.
			for tbl := range data {
				if group := tableToGroup[tbl]; group > 1 {
					for _, tableToCheck := range tableGroups[group-1] {
						a.Len(entriesByTable[tableToCheck], len(muts))
					}
				}
			}
			if !more {
				break
			}
		}
		for _, seen := range entriesByTable {
			if a.Len(seen, len(expectedMutOrder)) {
				a.Equal(expectedMutOrder, seen)
			}
		}
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
	fixture, cancel, err := sinktest.NewFixture()
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()

	ctx := fixture.Context
	targetDB := fixture.TestDB.Ident()

	dummyTarget := ident.NewTable(
		targetDB, ident.Public, ident.New("target"))

	s, err := fixture.Stagers.Get(ctx, dummyTarget)
	if err != nil {
		b.Fatal(err)
	}

	allBytes := int64(0)
	muts := sinktest.MutationGenerator(ctx, 100000, 0.5)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		batch := make([]types.Mutation, batchSize)
		for pb.Next() {
			for i := range batch {
				mut := <-muts
				batch[i] = mut
				atomic.AddInt64(&allBytes, int64(len(mut.Data)+len(mut.Key)))
			}
			if err := s.Store(ctx, fixture.Pool, batch); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Use JSON byte count as throughput measure.
	b.SetBytes(atomic.LoadInt64(&allBytes))

}
