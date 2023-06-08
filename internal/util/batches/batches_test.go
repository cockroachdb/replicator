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

package batches

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	tcs := []struct {
		count    int
		batch    int
		expected [][2]int
	}{
		{
			count:    3,
			batch:    1,
			expected: [][2]int{{0, 1}, {1, 2}, {2, 3}},
		},
		// Test an off-by one, where the count isn't evenly divisible by
		// the batch size.
		{
			count:    11,
			batch:    2,
			expected: [][2]int{{0, 2}, {2, 4}, {4, 6}, {6, 8}, {8, 10}, {10, 11}},
		},
	}

	defer func() { *batchSize = defaultSize }()
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			a := assert.New(t)
			*batchSize = tc.batch
			a.Equal(tc.batch, Size())

			var read [][2]int
			a.NoError(Batch(tc.count, func(begin, end int) error {
				read = append(read, [2]int{begin, end})
				return nil
			}))

			a.Equal(tc.expected, read)
		})
	}
}
