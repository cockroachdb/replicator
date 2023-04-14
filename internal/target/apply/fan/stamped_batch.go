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
	"os"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

var _, stampedBackSanityCheck = os.LookupEnv("STAMPED_BATCH_CHECKS")

// A stampedBatch of mutations.
type stampedBatch struct {
	stamp stamp.Stamp
	mu    struct {
		sync.Mutex
		immutable bool // If true, prevents coalescing.
		muts      []types.Mutation
		weight    int64
	}
}

var _ stamp.Coalescing = (*stampedBatch)(nil)

func newBatch(stamp stamp.Stamp, mut types.Mutation) *stampedBatch {
	ret := &stampedBatch{stamp: stamp}
	ret.mu.muts = []types.Mutation{mut}
	ret.mu.weight = mutationWeight(mut)
	return ret
}

// Coalesce allows a tail element to be merged if it has the same Stamp.
func (b *stampedBatch) Coalesce(tail stamp.Stamped) stamp.Stamped {
	other := tail.(*stampedBatch)
	if stamp.Compare(b.stamp, other.stamp) != 0 {
		return tail
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// If this stampedBatch is in the process of being flushed, we don't
	// want it to allow coalescing.
	if b.mu.immutable {
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
	maxCount := batches.Size() - len(b.mu.muts)
	toTransfer := other.mu.muts
	if maxCount >= len(toTransfer) {
		b.mu.muts = append(b.mu.muts, toTransfer...)
		b.mu.weight += other.mu.weight
		other.mu.muts = nil
		other.mu.weight = 0
		return nil
	}

	// For sanity check later.
	origWeight := b.mu.weight + other.mu.weight

	// Push the elements around.
	other.mu.muts = toTransfer[maxCount:]
	toTransfer = toTransfer[:maxCount]
	b.mu.muts = append(b.mu.muts, toTransfer...)

	for _, mut := range toTransfer {
		weight := mutationWeight(mut)
		b.mu.weight += weight
		other.mu.weight -= weight
	}

	// Sanity check the math and/or concurrent mutation.
	if stampedBackSanityCheck && origWeight != b.mu.weight+other.mu.weight {
		panic(errors.Errorf("unexpected weight %d vs %d", origWeight, b.mu.weight+other.mu.weight))
	}
	return other
}

// Pick marks the batch as immutable, to prevent coalescing, and returns
// the enclosed mutations.
func (b *stampedBatch) Pick() (muts []types.Mutation, weight int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.immutable = true

	if stampedBackSanityCheck {
		var check int64
		for _, mut := range b.mu.muts {
			check += mutationWeight(mut)
		}
		if check != b.mu.weight {
			panic(errors.Errorf("mismatch in picked weight: %d vs %d", check, b.mu.weight))
		}
	}

	return b.mu.muts, b.mu.weight
}

// Stamp implements stamp.Stamped.
func (b *stampedBatch) Stamp() stamp.Stamp { return b.stamp }

// Weight returns the backpressure weight of the batch.
func (b *stampedBatch) Weight() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.weight
}

// mutationWeight computes a weight to use for generating backpressure.
func mutationWeight(mut types.Mutation) int64 {
	return int64(len(mut.Data)) + int64(len(mut.Key))
}
