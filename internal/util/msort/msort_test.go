// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package msort

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/stretchr/testify/assert"
)

func mut(k int, v string, t ...hlc.Time) types.Mutation {
	ret := types.Mutation{
		Data: []byte(fmt.Sprintf(`{"key":%d, "value":%s}`, k, v)),
		Key:  []byte(fmt.Sprintf(`[%d]`, k)),
		Time: hlc.New(int64(k), k),
	}
	if len(t) >= 1 {
		ret.Time = t[0]
	}
	return ret
}

func TestUniqueByKey(t *testing.T) {
	tcs := []struct {
		data, expected []types.Mutation
	}{
		{data: nil, expected: nil},
		{data: []types.Mutation{}, expected: []types.Mutation{}},
		{data: []types.Mutation{mut(1, "1")}, expected: []types.Mutation{mut(1, "1")}},
		{
			data: []types.Mutation{
				mut(1, "deleted"),
				mut(1, "expected"),
			},
			expected: []types.Mutation{
				mut(1, "expected"),
			},
		},
		{
			data: []types.Mutation{
				mut(2, "expected"),
				mut(1, "deleted"),
				mut(1, "deleted"),
				mut(4, "expected"),
				mut(1, "deleted"),
				mut(1, "deleted"),
				mut(1, "deleted"),
				mut(1, "expected"),
				mut(3, "expected"),
			},
			expected: []types.Mutation{
				mut(2, "expected"),
				mut(4, "expected"),
				mut(1, "expected"),
				mut(3, "expected"),
			},
		},
		{
			data: []types.Mutation{
				mut(1, "deleted"),
				mut(2, "expected"),
				mut(1, "expected"),
			},
			expected: []types.Mutation{
				mut(2, "expected"),
				mut(1, "expected"),
			},
		},
		// Test the case where timestamps are out of order for a key.
		{
			data: []types.Mutation{
				mut(1, "expected", hlc.New(100, 100)),
				mut(2, "expected"),
				mut(1, "expected"),
			},
			expected: []types.Mutation{
				mut(2, "expected"),
				mut(1, "expected", hlc.New(100, 100)),
			},
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)

			data := UniqueByKey(tc.data)
			a.Equal(tc.expected, data)
		})
	}
}

// Document the panic behavior.
func TestUniqueByKeyPanic(t *testing.T) {
	a := assert.New(t)
	a.Panics(func() {
		UniqueByKey([]types.Mutation{
			{Key: nil},
		})
	})
}
