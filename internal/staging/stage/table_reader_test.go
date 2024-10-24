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

package stage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/sinktest/mutations"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestTableReader(t *testing.T) {
	const mutCount = 4096
	const fragmentSize = 100
	const expectedBatches = mutCount/fragmentSize + 1
	r := require.New(t)
	start := time.Now()

	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	info, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (pk int primary key)")
	r.NoError(err)

	f := ProvideFactory(
		&Config{}, fixture.StagingPool, fixture.StagingDB, fixture.Context,
	).(*factory)
	r.NoError(f.cfg.Preflight())
	stage, err := f.newStage(info.Name())
	r.NoError(err)

	// Insert single-row transactions and then a single large transaction.
	mutCh := mutations.Generator(ctx, 1024, 0)
	singleMuts := make([]types.Mutation, mutCount)
	for idx := range singleMuts {
		mut := <-mutCh
		mut.Time = hlc.New(int64(idx+1), idx+1)
		singleMuts[idx] = mut
	}
	smallBatchEnd := singleMuts[len(singleMuts)-1].Time
	r.NoError(stage.Stage(ctx, fixture.StagingPool, singleMuts))

	bigMuts := make([]types.Mutation, mutCount)
	bigTime := hlc.New(4*mutCount, 0)
	for idx := range bigMuts {
		mut := <-mutCh
		// The RNG will produce duplicate keys, and all mutations
		// are added at the same time. We need to use distinct keys
		// or some mutations would be deduplicated away.
		mut.Key = json.RawMessage(fmt.Sprintf("[ %d ]", len(bigMuts)+idx))
		mut.Time = bigTime
		bigMuts[idx] = mut
	}
	// Mutations at the same timestamp will be sorted by the database in
	// lexicographical order.
	sort.Slice(bigMuts, func(i, j int) bool {
		return bytes.Compare(bigMuts[i].Key, bigMuts[j].Key) < 0
	})
	r.NoError(stage.Stage(ctx, fixture.StagingPool, bigMuts))

	log.Infof("inserted data in %s", time.Since(start))

	bounds := notify.VarOf(hlc.Range{})
	out := make(chan *tableCursor)
	reader := newTableReader(
		bounds,
		fixture.StagingPool,
		fragmentSize,
		out,
		fixture.StagingDB.Schema(),
		info.Name())
	ctx.Go(func(ctx *stopper.Context) error {
		reader.run(ctx)
		return nil
	})

	t.Run("small-reads", func(t *testing.T) {
		r := require.New(t)

		var seen []types.Mutation
		var times []hlc.Time
		bounds.Set(hlc.RangeIncluding(hlc.Zero(), smallBatchEnd))
	loop:
		for {
			select {
			case cursor := <-out:
				r.NoError(cursor.Error)
				r.NotNil(cursor.Batch)
				r.False(cursor.Jump)

				// We expect the fragment bit to be set for all cursors but
				// the last. Each transaction is exactly one mutation long,
				// so the underlying query can't know that there isn't
				// actually another mutation in that transaction that would
				// be picked up in the next query.
				idx := len(times)
				if idx < expectedBatches-1 {
					r.True(cursor.Fragment)
				} else {
					r.False(cursor.Fragment)
				}

				seen = append(seen, types.Flatten(cursor.Batch)...)
				times = append(times, cursor.Progress.Min())

				log.Infof("position %s vs %s", cursor.Progress, smallBatchEnd.Next())
				if cursor.Progress.MaxInclusive() == smallBatchEnd {
					break loop
				}
			case <-ctx.Done():
				r.NoError(ctx.Err())
			}
		}
		r.Equal(len(singleMuts), len(seen))
		r.Equal(singleMuts, seen)
		r.Len(times, expectedBatches)
		select {
		case <-out:
			r.Fail("should not have any more data to read")
		default:
		}
	})

	// Move the window backwards should have no effect.
	t.Run("rewind", func(t *testing.T) {
		r := require.New(t)

		bounds.Set(hlc.RangeIncluding(hlc.Zero(), hlc.New(int64(mutCount/2), mutCount/2)))
		select {
		case cursor := <-out:
			r.NoError(cursor.Error)
			r.NotNil(cursor.Batch)
			r.Zero(cursor.Batch.Count())
			r.False(cursor.Fragment)
			r.False(cursor.Jump)

			// This should be unchanged from the preceding test.
			r.Equal(hlc.RangeIncluding(hlc.Zero(), smallBatchEnd), cursor.Progress)
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	})

	// Jump forward to the big transaction. We should see the jump
	// flag set on the initial read.
	t.Run("big-transaction", func(t *testing.T) {
		r := require.New(t)

		bounds.Set(hlc.RangeIncluding(bigTime, bigTime))
		var seen []types.Mutation
		// Should see exactly one timestamp.
		times := make(map[hlc.Time]struct{})
	loop:
		for {
			select {
			case cursor := <-out:
				r.NoError(cursor.Error)
				r.NotNil(cursor.Batch)

				// Expect a jump forward on the first round.
				if len(seen) == 0 {
					r.True(cursor.Jump)
				} else {
					r.False(cursor.Jump)
				}

				r.Len(cursor.Batch.Data, 1)
				times[cursor.Batch.Data[0].Time] = struct{}{}

				seen = append(seen, types.Flatten(cursor.Batch)...)

				log.Infof("Progress %s", cursor.Progress)
				if cursor.Progress.MaxInclusive() == bigTime {
					break loop
				}
			case <-ctx.Done():
				r.NoError(ctx.Err())
			}
		}
		r.Equal(len(bigMuts), len(seen))
		r.Equal(bigMuts, seen)
		r.Len(times, 1)
	})

	t.Run("advance-empty", func(t *testing.T) {
		r := require.New(t)

		// Slide the time window forward.
		jumpStart := hlc.New(int64(19*mutCount), 0)
		jumpEnd := hlc.New(int64(20*mutCount), 0)
		bounds.Set(hlc.RangeIncluding(jumpStart, jumpEnd))

		// We expect to see an empty scan that advances to the end.
		select {
		case cursor := <-out:
			r.NoError(cursor.Error)
			r.NotNil(cursor.Batch)
			r.Empty(cursor.Batch.Data)
			r.False(cursor.Fragment)
			r.Equal(jumpEnd, cursor.Progress.MaxInclusive())
			r.True(cursor.Jump)
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	})
}
