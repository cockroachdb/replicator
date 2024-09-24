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

package buffer

import (
	"container/ring"
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestState(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		checkState(t, 1)
	})
	t.Run("2", func(t *testing.T) {
		checkState(t, 2)
	})
	t.Run("3", func(t *testing.T) {
		checkState(t, 3)
	})
	t.Run("100", func(t *testing.T) {
		checkState(t, 100)
	})
}

func checkState(t *testing.T, size int) {
	r := require.New(t)

	expect := make([]*types.TemporalBatch, size)
	x := ring.New(size)
	s := &state{
		head: x,
		tail: x,
	}

	checkRead := func(from *readMarker, expect []*types.TemporalBatch) (resume *readMarker) {
		resume, found := s.Read(from, nil)
		r.Equal(expect, found)

		// Verify repeated read returns no results.
		again, empty := s.Read(resume, nil)
		r.Same(resume, again)
		r.Empty(empty)
		return resume
	}

	// Verify initial empty state.
	r.True(s.Empty())
	checkRead(nil, nil)

	// Add entries and expect to fill the ring.
	for i := range expect {
		expect[i] = &types.TemporalBatch{Time: hlc.New(int64(i+1), i)}
		r.Falsef(s.Full(), "idx %d", i)
		s.Append(expect[i])
		r.Falsef(s.Empty(), "idx %d", i)
	}
	r.True(s.Full())
	r.Same(s.tail, s.head)

	// Walk a cursor over the state to read the contents.
	resume := checkRead(nil, expect)

	// Drain the first entry.
	s.Drain(expect[0].Time.Next())
	r.False(s.Full())
	if size == 1 {
		r.True(s.Empty())
		r.Nil(s.Head())
	} else {
		r.False(s.Empty())
		r.Same(expect[1], s.Head())
	}
	expect = expect[1:]

	// Add one new entry.
	post := &types.TemporalBatch{Time: hlc.New(int64(size+1), size)}
	s.Append(post)

	// Verify continuing to read returns only the newly-added entry.
	resume = checkRead(resume, []*types.TemporalBatch{post})

	// Verify a new read returns all data.
	expect = append(expect, post)
	checkRead(nil, expect)

	// Drain all data.
	s.Drain(post.Time)
	r.True(s.Empty())
	resume = checkRead(resume, nil)
	checkRead(nil, nil)
}
