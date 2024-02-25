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
	"math"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/mutations"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutAndDrain will insert and dequeue a batch of Mutations.
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

	// Select all mutations to set the applied flag. This allows the
	// mutations to be deleted.
	cursor := &types.UnstageCursor{
		EndBefore:      endTime.Next(),
		Targets:        []ident.Table{dummyTarget},
		TimestampLimit: count / 10,
	}
	a.Equal(hlc.Zero(), cursor.MinOffset())

	unstagedCount := 0
	readBack := make([]types.Mutation, 0, len(muts))
	for unstaging := true; unstaging; {
		cursor, unstaging, err = fixture.Stagers.Unstage(ctx, pool, cursor,
			func(_ context.Context, _ ident.Table, mut types.Mutation) error {
				unstagedCount++
				readBack = append(readBack, mut)
				return nil
			})
		r.NoError(err)
	}
	a.Equal(total, unstagedCount)
	a.Equal(muts, readBack)
	a.Equal(endTime, cursor.MinOffset())

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

	dummyTarget := ident.NewTable(targetDB, ident.New("target"))

	s, err := fixture.Stagers.Get(ctx, dummyTarget)
	r.NoError(err)

	// Store a seed mutation.
	r.NoError(s.Stage(ctx, pool, []types.Mutation{
		{
			Data: json.RawMessage(`{pk:1}`),
			Key:  json.RawMessage(`[1]`),
			Time: hlc.New(1, 0),
		},
	}))

	found, err := fixture.PeekStaged(ctx, dummyTarget, hlc.Zero(), hlc.New(100, 0))
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

	pending, err := s.StageIfExists(ctx, pool, proposed)
	r.NoError(err)
	r.Equal([]types.Mutation{proposed[0], proposed[2]}, pending)

	found, err = fixture.PeekStaged(ctx, dummyTarget, hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(found, 2)

	// Bump the timestamp and try again. We should see the same
	// keys be selected.
	for idx := range proposed {
		proposed[idx].Time = hlc.New(3, 0)
	}

	pending, err = s.StageIfExists(ctx, pool, proposed)
	r.NoError(err)
	r.Equal([]types.Mutation{proposed[0], proposed[2]}, pending)

	found, err = fixture.PeekStaged(ctx, dummyTarget, hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(found, 3)

	// Unstage the mutations.
	var count int
	cursor := &types.UnstageCursor{
		StartAt:        hlc.Zero(),
		EndBefore:      hlc.New(100, 0),
		Targets:        []ident.Table{dummyTarget},
		TimestampLimit: 100,
	}
	_, read, err := fixture.Stagers.Unstage(ctx, pool, cursor, func(context.Context, ident.Table, types.Mutation) error {
		count++
		return nil
	})
	r.NoError(err)
	r.True(read)
	r.Equal(3, count)

	found, err = fixture.PeekStaged(ctx, dummyTarget, hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Empty(found)

	// Since the staging table is effectively empty, this call should be
	// a no-op.
	pending, err = s.StageIfExists(ctx, pool, proposed)
	r.NoError(err)
	r.Equal(proposed, pending)
}

func TestUnstage(t *testing.T) {
	const entries = 1_000 // Increase for ersatz perf testing.
	const tableCount = 10
	a := assert.New(t)
	r := require.New(t)

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
	const distinctTimestamps = 2 + 2*entries
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

	log.Info("finished filling data")

	// checkCount is a helper function to verify that the cursor returns
	// a specific number of mutations. The StartAfterKey field in the
	// cursor will be cleared and the timestamp limit will be raised to
	// a large value.
	checkCount := func(a *assert.Assertions, tx types.StagingQuerier, q *types.UnstageCursor, expectCount int) error {
		q = q.Copy()
		q.TimestampLimit = math.MaxInt32
		var count int
		for hasMore := true; hasMore; {
			var err error
			_, hasMore, err = fixture.Stagers.Unstage(ctx, tx, q,
				func(context.Context, ident.Table, types.Mutation) error {
					count++
					return nil
				})
			if err != nil {
				return err
			}
		}
		a.Equal(expectCount, count)
		return nil
	}

	// checkEmpty is a helper function to verify that the cursor returns
	// no data. The StartAfterKey field will be cleared.
	checkEmpty := func(ctx context.Context, tx types.StagingQuerier, q *types.UnstageCursor) error {
		q = q.Copy()
		q.TableOffsets = ident.TableMap[types.UnstageOffset]{}
		_, hasMore, err := fixture.Stagers.Unstage(ctx, tx, q,
			func(context.Context, ident.Table, types.Mutation) error {
				return errors.New("no mutations should be visible")
			})
		if err != nil {
			return err
		}
		if hasMore {
			return errors.New("should not have more mutations to return")
		}
		return nil
	}

	// unstage reads from the given cursor until no more data is
	// available. It returns the data for each table and the number of
	// times the unstaging callback function was invoked.
	unstage := func(ctx context.Context, tx types.StagingQuerier, q *types.UnstageCursor) (data *ident.TableMap[[]types.Mutation], numSelections int, _ error) {
		data = &ident.TableMap[[]types.Mutation]{}
		for selecting := true; selecting; {
			log.Infof("cursor: %s", q)
			q, selecting, err = fixture.Stagers.Unstage(ctx, tx, q,
				func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
					data.Put(tbl, append(data.GetZero(tbl), mut))
					return nil
				})
			if err != nil {
				return nil, 0, err
			}
			numSelections++
		}
		return
	}

	// Read the big batch at T=1 in small chunks. This verifies that
	// we can paginate correctly within a single timestamp.
	t.Run("transactional-incremental", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.UnstageCursor{
			StartAt:     hlc.New(1, 0),
			EndBefore:   hlc.New(2, 0),
			Targets:     tables,
			UpdateLimit: entries / 100,
		}

		// Run the select in a discarded transaction to avoid
		// contaminating future tests with side effects.
		r.NoError(retry.Retry(ctx, func(ctx context.Context) error {
			tx, err := fixture.StagingPool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return errors.WithStack(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()

			entriesByTable, numSelections, err := unstage(ctx, tx, q)
			if err != nil {
				return err
			}
			// Expect the big batch to be carved up, then one extra for
			// the empty read.
			a.Equal(100+1, numSelections)

			// We're going to look for each table to have read back
			// the etries at T=1.
			if err := entriesByTable.Range(func(_ ident.Table, seen []types.Mutation) error {
				if a.Len(seen, entries) {
					a.Equal(expectedMutOrder[:entries], seen)
				}
				return nil
			}); err != nil {
				return err
			}

			// Ensure a re-read returns no data.
			return checkEmpty(ctx, tx, q)
		}))
	})

	t.Run("all-limits", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.UnstageCursor{
			StartAt:        hlc.New(2, 0),          // Skip the initial transaction
			EndBefore:      hlc.New(10*entries, 1), // Read the second large batch
			Targets:        tables,
			TimestampLimit: entries / 10,
			UpdateLimit:    entries / 20,
		}

		// Run the select in a discarded transaction to avoid
		// contaminating future tests with side effects.
		r.NoError(retry.Retry(ctx, func(ctx context.Context) error {
			tx, err := fixture.StagingPool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return errors.WithStack(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			entriesByTable, numSelections, err := unstage(ctx, tx, q)
			if err != nil {
				return err
			}
			// We expect to see groups of 1/10th of the timestamps for
			// the individually-queued batches, then groups of mutations
			// within the large batch, and then a final empty update.
			a.Equal(2*10+20+1, numSelections)
			if err := entriesByTable.Range(func(_ ident.Table, seen []types.Mutation) error {
				if a.Len(seen, 2*entries) {
					a.Equal(expectedMutOrder[entries:3*entries], seen)
				}
				return nil
			}); err != nil {
				return err
			}

			// Ensure a re-read returns no data.
			return checkEmpty(ctx, tx, q)
		}))
	})

	// Ensure that having a lease on a staged row prevents it from being
	// read again.
	t.Run("lease", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		q := &types.UnstageCursor{
			EndBefore:      hlc.New(100*entries, 0), // Past any existing time.
			Targets:        tables,
			LeaseExpiry:    time.Now().Add(time.Hour),
			TimestampLimit: entries/10 - 1,
		}

		// Run the select in a discarded transaction to avoid
		// contaminating future tests with side effects.
		r.NoError(retry.Retry(ctx, func(ctx context.Context) error {
			tx, err := fixture.StagingPool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return errors.WithStack(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()

			entriesByTable, numSelections, err := unstage(ctx, tx, q)
			if err != nil {
				return err
			}

			// +1 each for non-divisible timestamp limit and the final, no-data callback.
			a.Equal(distinctTimestamps/q.TimestampLimit+2, numSelections)
			if err := entriesByTable.Range(func(_ ident.Table, seen []types.Mutation) error {
				if a.Len(seen, len(expectedMutOrder)) {
					a.Equal(expectedMutOrder, seen)
				}
				return nil
			}); err != nil {
				return err
			}

			// Ensure that a re-read returns no data.
			if err := checkEmpty(ctx, tx, q); err != nil {
				return err
			}

			// Peek at the table structure directly to ensure that we set
			// the lease column, but not the applied column.
			for _, table := range tables {
				var ct int
				q := fmt.Sprintf(
					`SELECT count(*) FROM %s WHERE NOT applied AND lease IS NOT NULL`,
					stagingTables.GetZero(table))
				if err := tx.QueryRow(ctx, q).Scan(&ct); err != nil {
					return errors.WithStack(err)
				}
				a.Equal(len(muts), ct, q)
			}

			// Sanity check the case where we deferred a mutation at
			// timestamp T, but where we might also have a staged mutation
			// at T+1. The presence of a lease at timestamp T should prevent
			// the update at T+1 from being returned. We'll manipulate the
			// table directly to set this case up.
			for _, table := range tables {
				q := fmt.Sprintf(`UPDATE %s SET lease = NULL WHERE nanos > 1`, stagingTables.GetZero(table))
				tag, err := tx.Exec(ctx, q)
				if err != nil {
					return errors.WithStack(err)
				}
				a.Equal(int64(3*entries), tag.RowsAffected())
			}
			if err := checkEmpty(ctx, tx, q); err != nil {
				return err
			}

			// Verify that setting the applied column on those T=1 mutations
			// will make the remainder visible.
			for _, table := range tables {
				q := fmt.Sprintf(`UPDATE %s SET applied=TRUE, lease=NULL WHERE nanos = 1`, stagingTables.GetZero(table))
				tag, err := tx.Exec(ctx, q)
				if err != nil {
					return errors.WithStack(err)
				}
				a.Equal(int64(entries), tag.RowsAffected())
			}
			if err := checkCount(a, tx, q, 3*entries*tableCount); err != nil {
				return err
			}

			// Mark all mutations as having been applied.
			if err := entriesByTable.Range(func(tbl ident.Table, muts []types.Mutation) error {
				stage, err := fixture.Stagers.Get(ctx, tbl)
				if err != nil {
					return err
				}
				return stage.MarkApplied(ctx, tx, muts)

			}); err != nil {
				return err
			}
			// Peek at the table structure directly to ensure that we
			// cleared and set the applied column.
			for _, table := range tables {
				var ct int
				q := fmt.Sprintf(
					`SELECT count(*) FROM %s WHERE applied AND lease IS NULL`,
					stagingTables.GetZero(table))
				if err := tx.QueryRow(ctx, q).Scan(&ct); err != nil {
					return errors.WithStack(err)
				}
				a.Equal(len(muts), ct, q)
			}

			// Ensure that a re-read returns no data.
			return checkEmpty(ctx, tx, q)
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
