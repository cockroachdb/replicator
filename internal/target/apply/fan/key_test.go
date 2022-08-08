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
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestKeyStability(t *testing.T) {
	tbl := ident.NewTable(ident.New("db"), ident.Public, ident.New("tbl"))
	shardCount := 16
	mut := types.Mutation{Key: []byte("some key")}

	k1 := keyFor(tbl, shardCount, mut)
	k2 := keyFor(tbl, shardCount, mut)
	assert.Equal(t, k1, k2)
}
