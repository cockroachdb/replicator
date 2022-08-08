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
	"hash/maphash"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

type bucketKey struct {
	table ident.Table
	shard int
}

// Use a fixed seed value when computing hashes so that a mutation for a
// specific key winds up in the same bucket.
var commonSeed = maphash.MakeSeed()

// keyFor produces a bucketKey to group mutations together by key.
// This function assumes that the encoding of the key is stable for
// any given row.
func keyFor(table ident.Table, shardCount int, mut types.Mutation) bucketKey {
	if shardCount <= 1 {
		return bucketKey{table, 0}
	}

	h := maphash.Hash{}
	h.SetSeed(commonSeed)
	_, _ = h.Write(mut.Key)
	shard := int(h.Sum64() % uint64(shardCount))
	return bucketKey{table, shard}
}
