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

package shingle

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
)

// segmentMultiBatch will split the batch into one or more batches whose
// mutation count is approximately idealSize. Excessively large
// [types.TemporalBatch] instances will not be split, but will instead
// be returned as singleton MultiBatches.
func segmentMultiBatch(batch *types.MultiBatch, idealSize int) []*types.MultiBatch {
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
