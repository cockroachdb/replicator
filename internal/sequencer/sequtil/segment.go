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

package sequtil

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// SegmentMultiBatch will split the batch into one or more batches whose
// mutation count is approximately idealSize. Excessively large
// [types.TemporalBatch] instances will not be split, but will instead
// be returned as singleton MultiBatches.
func SegmentMultiBatch(batch *types.MultiBatch, idealSize int) []*types.MultiBatch {
	count := batch.Count()
	if count <= idealSize {
		return []*types.MultiBatch{batch}
	}

	var segment *types.MultiBatch
	ret := make([]*types.MultiBatch, 0, count/idealSize+1) // Guess at capacity
	var total int

	for _, sub := range batch.Data {
		nextCount := sub.Count()
		if segment == nil || total+nextCount > idealSize {
			// Create a new segment if one does not exist or if adding
			// the next batch would exceed the ideal size.
			segment = &types.MultiBatch{
				Data:   []*types.TemporalBatch{sub},
				ByTime: map[hlc.Time]*types.TemporalBatch{sub.Time: sub},
			}
			ret = append(ret, segment)
			total = nextCount
		} else {
			// Otherwise, append to the current segment.
			segment.ByTime[sub.Time] = sub
			segment.Data = append(segment.Data, sub)
			total += nextCount
		}
	}

	return ret
}

// SegmentTableBatches will split the batch into one or more batches
// whose mutation count is approximately idealSize. All mutations for
// any given key will be contained in the same batch, even if that would
// make a batch over-large. This ensures that a row remains
// temporally-consistent, regardless of the success or failure of any
// given segment. The returned TableBatches will likely have mixed
// timestamps.
func SegmentTableBatches(
	batch *types.MultiBatch, table ident.Table, idealSize int,
) []*types.TableBatch {
	count := batch.Count()

	// Start by grouping the mutations by keys.
	grouped := make(map[string][]types.Mutation, count)
	for _, sub := range batch.Data {
		if tableBatch, ok := sub.Data.Get(table); ok {
			for _, mut := range tableBatch.Data {
				key := string(mut.Key)
				grouped[key] = append(grouped[key], mut)
			}
		}
	}

	ret := make([]*types.TableBatch, 0, count/idealSize+1) // Guess at capacity
	var segment *types.TableBatch
	var segmentCount int
	for _, muts := range grouped {
		nextCount := len(muts)
		if segment == nil || segmentCount+nextCount > idealSize {
			// Create a new segment if one does not exist or if adding
			// the next batch would exceed the ideal size.
			segment = &types.TableBatch{
				Data:  muts,
				Table: table,
				// Time is indefinite.
			}
			ret = append(ret, segment)
			segmentCount = nextCount
		} else {
			// Otherwise, append to the current segment.
			segment.Data = append(segment.Data, muts...)
			segmentCount += nextCount
		}
	}

	return ret
}
