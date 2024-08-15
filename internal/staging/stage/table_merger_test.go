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

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/sinktest/mutations"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestTableMerger(t *testing.T) {
	const mutCount = 1024
	const fragmentSize = 100
	const tableCount = 10
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	tables := make([]ident.Table, tableCount)
	stages := make([]*stage, tableCount)
	for idx := range tables {
		info, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (pk int primary key)")
		r.NoError(err)
		tables[idx] = info.Name()

		stage, err := newStage(ctx, fixture.StagingPool, fixture.StagingDB.Schema(), info.Name())
		r.NoError(err)
		stages[idx] = stage
	}

	mutCh := mutations.Generator(ctx, 1024, 0)

	// Insert single-row transactions and then a single large transaction.
	singleMuts := make([]types.Mutation, 0, tableCount*mutCount)
	bigMuts := make([]types.Mutation, 0, tableCount*mutCount)
	bigTime := hlc.New(4*mutCount, 0)
	for stageIdx, stage := range stages {
		stageMuts := make([]types.Mutation, mutCount)
		for idx := range stageMuts {
			mut := <-mutCh
			mut.Time = hlc.New(int64(idx+1), stageIdx+1)
			stageMuts[idx] = mut
		}
		r.NoError(stage.Stage(ctx, fixture.StagingPool, stageMuts))
		singleMuts = append(singleMuts, stageMuts...)

		stageMuts = make([]types.Mutation, mutCount)
		for idx := range stageMuts {
			mut := <-mutCh
			// The RNG will produce duplicate keys, and all mutations
			// are added at the same time. We need to use distinct keys
			// or some mutations would be deduplicated away.
			mut.Key = json.RawMessage(fmt.Sprintf("[ %d ]", len(bigMuts)+idx))
			mut.Time = bigTime
			stageMuts[idx] = mut
		}
		r.NoError(stage.Stage(ctx, fixture.StagingPool, stageMuts))
		bigMuts = append(bigMuts, stageMuts...)
	}

	sortMuts := func(x []types.Mutation) {
		sort.Slice(x, func(i, j int) bool {
			if c := hlc.Compare(x[i].Time, x[j].Time); c != 0 {
				return c < 0
			}
			return bytes.Compare(x[i].Key, x[j].Key) < 0
		})
	}

	sortMuts(singleMuts)
	sortMuts(bigMuts)
	smallBatchEnd := singleMuts[len(singleMuts)-1].Time

	bounds := notify.VarOf(hlc.RangeIncluding(hlc.Zero(), smallBatchEnd))

	sources := make([]<-chan *tableCursor, tableCount)
	for idx, table := range tables {
		out := make(chan *tableCursor, 1)
		sources[idx] = out
		reader := newTableReader(
			bounds,
			fixture.StagingPool,
			fragmentSize,
			out,
			fixture.StagingDB.Schema(),
			table)
		ctx.Go(func(ctx *stopper.Context) error {
			reader.run(ctx)
			return nil
		})
	}

	out := make(chan *types.StagingCursor)
	merge := newTableMerger(&types.TableGroup{
		Name:      ident.New("testing"),
		Enclosing: fixture.TargetSchema.Schema(),
		Tables:    tables,
	}, sources, out)
	ctx.Go(func(ctx *stopper.Context) error {
		merge.run(ctx)
		return nil
	})

	t.Run("merge-small", func(t *testing.T) {
		r := require.New(t)
		var batchRatchet hlc.Time
		var seen []types.Mutation

	loop:
		for {
			select {
			case cursor := <-out:
				r.NoError(cursor.Error)
				r.False(cursor.Jump)
				batch := cursor.Batch

				if batch != nil {
					seen = append(seen, types.Flatten[*types.TemporalBatch](batch)...)

					// The fragment bit will be set on boundary-aligned reads.
					if batch.Time.Nanos()%fragmentSize == 0 {
						r.True(cursor.Fragment)
					} else {
						r.False(cursor.Fragment)
					}

					// Ensure that time doesn't go backwards.
					r.True(hlc.Compare(cursor.Progress.MaxInclusive(), batchRatchet) >= 0)
					batchRatchet = cursor.Progress.MaxInclusive()
				}

				if cursor.Progress.MaxInclusive() == smallBatchEnd {
					// We're done.
					break loop
				}
			case <-ctx.Done():
				r.NoError(ctx.Err())
			}
		}
		r.Equal(len(singleMuts), len(seen))
		// Iteration order across tables varies.
		sortMuts(seen)
		r.Equal(singleMuts, seen)
	})

	// Jump forward to read the big transaction. We should see a
	// bunch of fragment-sized updates.
	t.Run("merge-big", func(t *testing.T) {
		r := require.New(t)
		bounds.Set(hlc.RangeIncluding(bigTime, bigTime))
		var batchRatchet hlc.Time
		var seen []types.Mutation

	loop:
		for {
			select {
			case cursor := <-out:
				r.NoError(cursor.Error)
				batch := cursor.Batch
				if batch != nil {

					seen = append(seen, types.Flatten[*types.TemporalBatch](cursor.Batch)...)
				}

				// Ensure that time doesn't go backwards.
				r.True(hlc.Compare(cursor.Progress.MaxInclusive(), batchRatchet) >= 0)
				batchRatchet = cursor.Progress.MaxInclusive()

				if cursor.Progress.MaxInclusive() == bigTime {
					break loop
				}
			case <-ctx.Done():
				r.NoError(ctx.Err())
			}
		}
		r.Equal(len(bigMuts), len(seen))
		// Iteration order across tables varies.
		sortMuts(seen)
		r.Equal(bigMuts, seen)
	})

	// Moving the lower or upper bounds backwards will have no effect.
	// We'll expect to see a progress-only update that reflects the
	// current state of the merge.
	t.Run("jump-backwards", func(t *testing.T) {
		r := require.New(t)
		jumpEnd := hlc.New(int64(2*mutCount), 0)
		bounds.Set(hlc.RangeIncluding(hlc.New(1, 0), jumpEnd))
		select {
		case cursor := <-out:
			r.NoError(cursor.Error)
			r.False(cursor.Jump)
			r.Equal(hlc.RangeIncluding(hlc.Zero(), bigTime), cursor.Progress)
			r.Nil(cursor.Batch)
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	})
}
