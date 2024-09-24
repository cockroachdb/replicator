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

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
)

type readMarker struct {
	from   *ring.Ring
	unless *types.TemporalBatch
}

// state maintains a linked-list buffer of mutations to replay in order
// to satisfy the [types.BatchReader.Read] contract.
type state struct {
	// Acts as a ratchet based on highest timestamp in the buffer.
	highWater hlc.Time
	// Begin replay after this ring entry. This pointer is advanced by
	// the cleanup routine, based on the high-water mark.
	head *ring.Ring
	// marked prevents duplicate delivery of mutations.
	marked map[hlc.Time]struct{}
	// Append new elements at this node.
	tail *ring.Ring
}

// Append the batch to the ring. If the frontend redelivers data we've
// already seen (e.g. network glitch forces reconnection to the source),
// this method will ignore it. This requires that the source has a
// strictly monotonic clock sequence (e.g. [hlc.Clock]).
func (s *state) Append(batch *types.TemporalBatch) {
	if s.tail.Value != nil {
		panic("ring is overfull")
	}

	if hlc.Compare(batch.Time, s.highWater) <= 0 {
		return
	}

	s.highWater = batch.Time
	s.tail.Value = batch
	s.tail = s.tail.Next()
}

// Drain removes all elements from the head of the ring whose timestamps
// are less than or equal to highWater.
func (s *state) Drain() bool {
	drained := false
	for head := s.Head(); head != nil; head = s.Head() {
		// Retain entries after the mark.
		if hlc.Compare(head.Time, s.highWater) > 0 {
			break
		}
		delete(s.marked, head.Time)
		// Clear previous value.
		s.head.Value = nil
		// Advance the pointer.
		s.head = s.head.Next()
		drained = true
	}
	return drained
}

// Empty returns true if there are no elements in the ring.
func (s *state) Empty() bool {
	return s.head.Value == nil
}

// Full returns true if the ring is full.
func (s *state) Full() bool {
	return s.tail.Value != nil
}

// Head returns the batch at the head of the ring, or nil if the ring is
// empty.
func (s *state) Head() *types.TemporalBatch {
	ptr, _ := s.head.Value.(*types.TemporalBatch)
	return ptr
}

// Mark the batch has having been processed. This will prevent it from
// being re-delivered.
func (s *state) Mark(ts hlc.Time) {
	s.marked[ts] = struct{}{}
}

// Read appends values starting from the given ring entry. If from is
// nil, the earliest entry will be returned first.
func (s *state) Read(mark *readMarker, into []*types.TemporalBatch) (*readMarker, []*types.TemporalBatch) {
	if mark == nil {
		mark = &readMarker{from: s.head}
	}

	didChange := false
	resume := *mark
	for {
		batch, ok := resume.from.Value.(*types.TemporalBatch)
		if !ok || batch == mark.unless {
			break
		}
		if _, accepted := s.marked[batch.Time]; !accepted {
			didChange = true
			into = append(into, batch)
		}
		resume.from = resume.from.Next()
		resume.unless, _ = resume.from.Value.(*types.TemporalBatch)

		// Stop when we have hit the last element.
		if resume.from == s.tail {
			break
		}
	}
	if !didChange {
		return mark, into
	}
	return &resume, into
}
