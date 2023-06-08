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
