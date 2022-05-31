// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fan

import (
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
)

// A stampedBatch of mutations.
type stampedBatch struct {
	stamp stamp.Stamp
	mu    struct {
		sync.Mutex
		immutable bool // If true, prevents coalescing.
		muts      []types.Mutation
	}
}

var _ stamp.Coalescing = (*stampedBatch)(nil)

func newBatch(stamp stamp.Stamp, mut types.Mutation) *stampedBatch {
	ret := &stampedBatch{stamp: stamp}
	ret.mu.muts = []types.Mutation{mut}
	return ret
}

// Coalesce allows a tail element to be merged if it has the same Stamp.
func (b *stampedBatch) Coalesce(tail stamp.Stamped) stamp.Stamped {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If this stampedBatch is in the process of being flushed, we don't
	// want it to allow coalescing.
	if b.mu.immutable {
		return tail
	}

	other := tail.(*stampedBatch)
	if stamp.Compare(b.stamp, other.stamp) != 0 {
		return tail
	}

	// This is really for pedantry's sake, since we're reaching into
	// the mu struct. The tail value being passed in shouldn't be
	// visible to any other goroutine, since the datastructure is
	// single-threaded through the owning bucket's mutex.
	other.mu.Lock()
	defer other.mu.Unlock()

	// This is not an expected case.
	if other.mu.immutable {
		return tail
	}

	// Append as many elements as will bring us up to the configured
	// batching size.
	toAppend := other.mu.muts
	maxCount := batches.Size() - len(b.mu.muts)
	if maxCount > len(toAppend) {
		maxCount = len(toAppend)
	}
	b.mu.muts = append(b.mu.muts, toAppend[:maxCount]...)

	// Return the remainder, or nil if we've consumed all elements.
	toAppend = toAppend[maxCount:]
	if len(toAppend) > 0 {
		other.mu.muts = toAppend
		return other
	}
	return nil
}

// Pick marks the batch as immutable, to prevent coalescing, and returns
// the enclosed mutations.
func (b *stampedBatch) Pick() []types.Mutation {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.immutable = true
	return b.mu.muts
}

// Stamp implements stamp.Stamped.
func (b *stampedBatch) Stamp() stamp.Stamp { return b.stamp }
