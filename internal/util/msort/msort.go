// Copyright 2023 The Cockroach Authors
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

// Package msort contains utility functions for sorting and
// de-duplicating batches of mutations.
package msort

import (
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
)

// UniqueByKey implements a "last one wins" approach to removing
// mutations with duplicate keys from the input slice. If two mutations
// share the same Key, then the one with the later Time is returned. If
// there are mutations with identical Keys and Times, exactly one of the
// values will be chosen arbitrarily.
//
// A new slice is returned.
//
// This function will panic if any of the mutation Key fields are
// entirely empty. An empty json array (i.e. `[]`) is acceptable.
func UniqueByKey(x []types.Mutation) []types.Mutation {
	return uniqueBy(x,
		func(e types.Mutation) string {
			key := string(e.Key)
			// This is a sanity-check to ensure that we don't silently
			// discard mutations due to some upstream coding error where a
			// mutation does not have its Key field set.
			if len(key) == 0 {
				panic("empty key")
			}
			return key
		},
		func(existing, matching types.Mutation) types.Mutation {
			if hlc.Compare(existing.Time, matching.Time) >= 0 {
				return existing
			}
			return matching
		},
	)
}

// UniqueByTimeKey implements a "last one wins" approach to removing
// mutations with duplicate (time, key) tuples from the input slice. If
// two mutations share the same (time, key) pair, then the one later in
// the input slice is returned.
//
// A new slice is returned.
//
// This function will panic if any of the mutation Key fields are
// entirely empty. An empty json array (i.e. `[]`) is acceptable.
func UniqueByTimeKey(x []types.Mutation) []types.Mutation {
	return uniqueBy(x,
		func(e types.Mutation) string {
			key := string(e.Key)
			if len(key) == 0 {
				panic("empty key")
			}
			return fmt.Sprintf("%s:%s", e.Time, key)
		},
		func(existing, _ types.Mutation) types.Mutation {
			// Return existing since we iterate backwards.
			return existing
		},
	)
}

func uniqueBy[T ~[]E, E any, C comparable](
	x T, keyFn func(e E) C, pickFn func(existing, proposed E) E,
) T {
	// Make a copy of the data, to avoid side-effect pollution.
	x = append(T(nil), x...)

	// For any given Key, we're going to track the index in the slice
	// that holds data for the key.
	seenIdx := make(map[C]int, len(x))

	// We want to iterate backwards over the input slice, moving
	// elements to the rear when their HLC time is greater than the
	// value currently tracked for that key.
	dest := len(x)
	for src := len(x) - 1; src >= 0; src-- {
		key := keyFn(x[src])

		// Is there already an index in the slice for that key?
		if curIdx, found := seenIdx[key]; found {
			// If so, replace the value if the HLC time is greater.
			x[curIdx] = pickFn(x[curIdx], x[src])
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
