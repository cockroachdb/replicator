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
	"context"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/mutations"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestSegmentMultiBatch(t *testing.T) {
	const count = 1000
	const ideal = 99
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	table := ident.NewTable(
		ident.MustSchema(ident.New("foo")),
		ident.New("bar"))

	// Generate a large batch of data.
	big := &types.MultiBatch{}
	mutCh := mutations.Generator(ctx, 1024, 0)
	for i := 0; i < count; i++ {
		r.NoError(big.Accumulate(table, <-mutCh))
	}
	r.Equal(count, big.Count())

	// Verify number of generated segments.
	segments := SegmentMultiBatch(big, ideal)
	r.Len(segments, count/ideal+1)

	// Ensure we haven't dropped any mutations.
	total := 0
	for _, segment := range segments {
		total += segment.Count()
	}
	r.Equal(count, total)

	// Verify small-segment pass-through.
	moreSegments := SegmentMultiBatch(segments[0], ideal)
	r.Len(moreSegments, 1)
	r.Same(segments[0], moreSegments[0])
}

func TestSegmentTableBatches(t *testing.T) {
	const count = 1000
	const ideal = 99
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	table := ident.NewTable(
		ident.MustSchema(ident.New("foo")),
		ident.New("bar"))

	// Generate a large batch of data, ensuring that we'll repeat keys.
	big := &types.MultiBatch{}
	mutCh := mutations.Generator(ctx, count/2, 0)
	for i := 0; i < count; i++ {
		r.NoError(big.Accumulate(table, <-mutCh))
	}
	r.Equal(count, big.Count())

	// Verify number of generated segments.
	segments := SegmentTableBatches(big, table, ideal)
	r.Len(segments, count/ideal+1)

	// Ensure that keys are grouped, we haven't dropped any mutations,
	// and that time order is maintained.
	group := make(map[string]*types.TableBatch, count/2)
	times := make(map[string]hlc.Time, count/2)
	total := 0
	for _, segment := range segments {
		for _, mut := range segment.Data {
			key := string(mut.Key)
			if found, ok := group[key]; ok {
				r.Same(segment, found)
			} else {
				group[key] = segment
			}
			r.True(hlc.Compare(mut.Time, times[key]) > 0)
			times[key] = mut.Time
		}
		total += segment.Count()
	}
	r.Equal(count, total)
}
