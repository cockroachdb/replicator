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

	// Insert.
	a.NoError(s.Store(ctx, fixture.Pool, muts))

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
