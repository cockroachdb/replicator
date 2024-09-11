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
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/stretchr/testify/assert"
)

func mut(k int, v string, t ...hlc.Time) types.Mutation {
	ret := types.Mutation{
		Key:  []byte(fmt.Sprintf(`[%d]`, k)),
		Time: hlc.New(int64(k), k),
	}
	if v == "deleted" {
		ret.Deletion = true
	} else {
		ret.Data = []byte(fmt.Sprintf(`{"key":%d,"value":%q}`, k, v))
	}
	if len(t) >= 1 {
		ret.Time = t[0]
	}
	return ret
}

func TestFoldByKey(t *testing.T) {
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
				mut(1, "expected"),
				mut(2, "expected"),
				mut(3, "expected"),
				mut(4, "expected"),
			},
		},
		{
			data: []types.Mutation{
				mut(1, "deleted"),
				mut(2, "expected"),
				mut(1, "expected"),
			},
			expected: []types.Mutation{
				mut(1, "expected"),
				mut(2, "expected"),
			},
		},
		{
			data: []types.Mutation{
				mut(1, "deleted"),
				mut(2, "expected"),
				mut(1, "expected"),
				mut(1, "deleted"),
			},
			expected: []types.Mutation{
				mut(1, "deleted"),
				mut(2, "expected"),
			},
		},
		// Test the case where timestamps are out of order for a key.
		{
			data: []types.Mutation{
				mut(1, "expected100", hlc.New(100, 100)),
				mut(2, "expected"),
				mut(1, "expected1"),
			},
			expected: []types.Mutation{
				mut(1, "expected100", hlc.New(100, 100)),
				mut(2, "expected"),
			},
		},
		// Verify property aggregation and sort stability.
		{
			data: []types.Mutation{
				{
					Key:  json.RawMessage(`[1]`),
					Data: json.RawMessage(`{"k":1,"a":-999}`),
				},
				{
					Key:  json.RawMessage(`[1]`),
					Data: json.RawMessage(`{"k":1,"b":2}`),
					Time: hlc.New(100, 100),
				},
				{
					Key:  json.RawMessage(`[1]`),
					Data: json.RawMessage(`{"k":1,"a":1,"c":3}`),
				},
			},
			expected: []types.Mutation{
				{
					Key:  json.RawMessage(`[1]`),
					Data: json.RawMessage(`{"a":1,"b":2,"c":3,"k":1}`),
					Time: hlc.New(100, 100),
				},
			},
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			originalData := slices.Clone(tc.data)
			data, err := FoldByKey(tc.data)
			a.NoError(err)
			a.Equal(tc.expected, data)
			// Verify function doesn't damage the input slice.
			a.Equal(originalData, tc.data)
		})
	}
}

// Ensure error case if a mutation has no key.
func TestFoldNoKey(t *testing.T) {
	a := assert.New(t)
	_, err := FoldByKey([]types.Mutation{
		mut(1, "ok"),
		{Key: nil},
		mut(2, "ok"),
	})
	a.ErrorContains(err, "mutation key must not be empty")
}

func TestUniqueByTimeKey(t *testing.T) {
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
				mut(1, "expected", hlc.New(100, 100)),
				mut(2, "expected"),
				mut(1, "expected"),
			},
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)

			data := UniqueByTimeKey(tc.data)
			a.Equal(tc.expected, data)
		})
	}
}
