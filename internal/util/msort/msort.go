// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package msort contains utility functions for sorting and
// de-duplicating batches of mutations.
package msort

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
)

// UniqueByKey implements a "last one wins" approach to removing
// mutations with duplicate keys from the input slice. If two mutations
// share the same Key, then the one with the later Time is returned. If
// there are mutations with identical Keys and Times, exactly one of the
// values will be chosen arbitrarily.
//
// The modified slice is returned.
//
// This function will panic if any of the mutation Key fields are
// entirely empty. An empty json array (i.e. `[]`) is acceptable.
func UniqueByKey(x []types.Mutation) []types.Mutation {
	// For any given Key, we're going to track the index in the slice
	// that holds data for the key.
	seenIdx := make(map[string]int, len(x))

	// We want to iterate backwards over the input slice, moving
	// elements to the rear when their HLC time is greater than the
	// value currently tracked for that key.
	dest := len(x)
	for src := len(x) - 1; src >= 0; src-- {
		// This is a sanity-check to ensure that we don't silently
		// discard mutations due to some upstream coding error where a
		// mutation does not have its Key field set.
		if len(x[src].Key) == 0 {
			panic("empty mutation key")
		}
		key := string(x[src].Key)

		// Is there already an index in the slice for that key?
		if curIdx, found := seenIdx[key]; found {
			// If so, replace the value if the HLC time is greater.
			if hlc.Compare(x[src].Time, x[curIdx].Time) > 0 {
				x[curIdx] = x[src]
			}
		} else {
			// Otherwise, allocate a new index for that key, and copy
			// the value out.
			dest--
			seenIdx[key] = dest
			x[dest] = x[src]
		}
	}

	// Return the compacted view of the slice.
	return x[dest:]
}
