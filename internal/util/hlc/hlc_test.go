// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hlc

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	a := assert.New(t)

	a.True(Compare(Time{1, 1}, Time{1, 1}) == 0)

	a.True(Compare(Time{2, 1}, Time{1, 1}) > 0)
	a.True(Compare(Time{1, 1}, Time{2, 1}) < 0)

	a.True(Compare(Time{1, 2}, Time{1, 1}) > 0)
	a.True(Compare(Time{1, 1}, Time{1, 2}) < 0)
}

func TestParse(t *testing.T) {
	// Implementation copied from sink_table_test.go

	tests := []struct {
		testcase        string
		expectedPass    bool
		expectedNanos   int64
		expectedLogical int
	}{
		{"", false, 0, 0},
		{".", false, 0, 0},
		{"1233", false, 0, 0},
		{".1233", false, 0, 0},
		{"123.123", false, 123, 123},
		{"0.0", false, 0, 0},
		{"1586019746136571000.0000000000", true, 1586019746136571000, 0},
		{"1586019746136571000.0000000001", true, 1586019746136571000, 1},
		{"9223372036854775807.2147483647", true, math.MaxInt64, math.MaxInt32},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d - %s", i, test.testcase), func(t *testing.T) {
			a := assert.New(t)
			actual, actualErr := Parse(test.testcase)
			if test.expectedPass && a.NoError(actualErr) {
				a.Equal(test.expectedNanos, actual.Nanos(), "nanos")
				a.Equal(test.expectedLogical, actual.Logical(), "logical")
				a.Equal(test.testcase, actual.String())
			} else if !test.expectedPass {
				a.Error(actualErr)
			}
		})
	}
}
