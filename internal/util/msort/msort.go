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
	"sort"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
)

// ByTime sorts the slice of mutations by their HLC timestamp.
func ByTime(x []types.Mutation) {
	sort.Slice(x, func(i, j int) bool {
		return hlc.Compare(x[i].Time, x[j].Time) < 0
	})
}

// UniqueByKey implements a "last one wins" approach to removing
// mutations with duplicate keys from the input slice. The modified
// slice is returned.
//
// This function will panic if any of the mutation Key fields are
// entirely empty. An empty json array (i.e. `[]`) is acceptable.
func UniqueByKey(x []types.Mutation) []types.Mutation {
	// We want to iterate backwards over the input slice, moving
	// elements to the rear when their keys are distinct. At the end,
	// we return a shortened view over the original backing array.
	seen := make(map[string]struct{}, len(x))
	dest := len(x)
	for src := len(x) - 1; src >= 0; src-- {
		// This is a sanity-check to ensure that we don't silently
		// discard mutations due to some upstream coding error where a
		// mutation does not have its Key field set.
		if len(x[src].Key) == 0 {
			panic("empty mutation key")
		}
		key := string(x[src].Key)
		if _, found := seen[key]; !found {
			seen[key] = struct{}{}
			dest--
			x[dest] = x[src]
		}
	}

	return x[dest:]
}
