// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
