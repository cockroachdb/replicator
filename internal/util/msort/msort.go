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
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/pkg/errors"
)

// FoldByKey implements a folding approach to removing mutations with
// duplicate keys from the input slice. If two mutations share the same
// Key, their data blocks will be merged on a key-by-key basis.
// Mutations will be processed in time order.
//
// This function will not modify the input slice, but may return it if
// all mutations were applied to unique keys.
func FoldByKey(x []types.Mutation) ([]types.Mutation, error) {
	// Handle trivial cases.
	if len(x) <= 1 {
		return x, nil
	}
	// Ensure that values in the slice are well-ordered. This should be
	// the general case.
	sorted := slices.IsSortedFunc(x, func(a, b types.Mutation) int {
		return hlc.Compare(a.Time, b.Time)
	})
	if !sorted {
		x = slices.Clone(x)
		slices.SortStableFunc(x, func(a, b types.Mutation) int {
			return hlc.Compare(a.Time, b.Time)
		})
	}
	exemplars := make(map[string]*types.Mutation, len(x))
	keyData := make(map[string]map[string]json.RawMessage, len(x))

	for idx, mut := range x {
		if len(mut.Key) == 0 {
			return nil, errors.New("mutation key must not be empty")
		}
		key := string(mut.Key)
		exemplar, collision := exemplars[key]

		// Last-one-wins for any other metadata (i.e. deletions).
		exemplars[key] = &x[idx]

		// Do nothing until we see a collision on the key. This should
		// be the general case.
		if !collision {
			continue
		}

		// The previous mutation for this key was a deletion, we can
		// just keep the current mutation.
		if exemplar.IsDelete() {
			continue
		}

		// Drop previous merges if the new key is a deletion.
		if mut.IsDelete() {
			delete(keyData, key)
			continue
		}

		// We're in a condition that requires folding.
		acc, exemplarUnpacked := keyData[key]

		// If this is first time we encounter a collision, unpack the
		// exemplar into the map. If the previous value was a deletion,
		// we don't need to do anything further.
		if !exemplarUnpacked {
			acc = make(map[string]json.RawMessage)
			keyData[key] = acc

			dec := json.NewDecoder(bytes.NewReader(exemplar.Data))
			dec.UseNumber()
			if err := dec.Decode(&acc); err != nil {
				return nil, errors.WithStack(err)
			}
		}

		// Patch the map with the next mutation.
		dec := json.NewDecoder(bytes.NewReader(mut.Data))
		dec.UseNumber()
		if err := dec.Decode(&acc); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	// If there were no collisions and no deletions, return the original
	// slice. This should be the general case.
	if len(keyData) == 0 && len(exemplars) == len(x) {
		return x, nil
	}

	// Reencode the folded mutations.
	ret := make([]types.Mutation, 0, len(exemplars))
	for key, exemplar := range exemplars {
		merged, collision := keyData[key]
		if !collision {
			ret = append(ret, *exemplar)
			continue
		}
		var err error
		mut := *exemplar // Don't modify elements in original slice.
		mut.Data, err = json.Marshal(merged)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ret = append(ret, mut)
	}
	// Sort the output by key.
	slices.SortFunc(ret, func(a, b types.Mutation) int {
		return bytes.Compare(a.Key, b.Key)
	})
	return ret, nil
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
	if x == nil {
		return nil
	}
	// Make a copy of the data, to avoid side-effect pollution.
	x = append(T{}, x...)

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
