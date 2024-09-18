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

package hlc

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBefore(t *testing.T) {
	a := assert.New(t)

	a.Equal(New(1, 0), New(1, 1).Before())
	a.Equal(New(1, math.MaxInt32), New(2, 0).Before())
}

func TestCompare(t *testing.T) {
	a := assert.New(t)

	a.True(Compare(New(1, 1), New(1, 1)) == 0)

	a.True(Compare(New(2, 1), New(1, 1)) > 0)
	a.True(Compare(New(1, 1), New(2, 1)) < 0)

	a.True(Compare(New(1, 2), New(1, 1)) > 0)
	a.True(Compare(New(1, 1), New(1, 2)) < 0)
}

func TestExtend(t *testing.T) {
	a := assert.New(t)

	hundred := New(100, 100)

	rng := RangeEmptyAt(hundred)
	a.False(rng.Contains(hundred))

	rng = rng.Extend(hundred)
	a.True(rng.Contains(hundred))
	a.Equal(RangeIncluding(hundred, hundred), rng)

	one := New(1, 1)
	a.False(rng.Contains(one))

	rng = rng.Extend(one)
	a.True(rng.Contains(one))
	a.Equal(RangeIncluding(one, hundred), rng)

	thousand := New(1000, 1000)
	a.False(rng.Contains(thousand))

	rng = rng.Extend(thousand)
	a.True(rng.Contains(thousand))
	a.Equal(RangeIncluding(one, thousand), rng)

	// No-op.
	a.Equal(rng, rng.Extend(New(500, 500)))
}

func TestNext(t *testing.T) {
	a := assert.New(t)

	a.Equal(New(1, 1), New(1, 0).Next())
}

func TestContains(t *testing.T) {
	a := assert.New(t)

	zero := Zero()
	nine := New(9, 0)
	ten := New(10, 0)
	almostTen := ten.Before()
	justOverTen := ten.Next()
	fifteen := New(15, 0)
	twenty := New(20, 0)
	almostTwenty := twenty.Before()
	justOverTwenty := twenty.Next()
	thirty := New(30, 0)
	max := New(math.MaxInt64, math.MaxInt32)
	// Create the smallest range that includes ten and twenty.
	rng := RangeIncluding(ten, twenty)

	a.False(rng.Contains(zero))
	a.False(rng.Contains(nine))
	a.False(rng.Contains(almostTen))
	a.True(rng.Contains(ten))
	a.True(rng.Contains(justOverTen))
	a.True(rng.Contains(fifteen))
	a.True(rng.Contains(almostTwenty))
	a.True(rng.Contains(twenty))
	a.False(rng.Contains(justOverTwenty))
	a.False(rng.Contains(thirty))
	a.False(rng.Contains(max))
}

func TestEmpty(t *testing.T) {
	a := assert.New(t)

	a.True(RangeEmpty().Empty())
	a.True(RangeEmptyAt(New(1, 0)).Empty())

	a.False(RangeIncluding(New(1, 0), New(1, 0)).Empty())
	a.False(RangeIncluding(New(1, 0), New(1, 1)).Empty())
}

func TestWithMin(t *testing.T) {
	a := assert.New(t)

	five := New(5, 0)
	ten := New(10, 0)
	twenty := New(20, 0)
	thirty := New(30, 0)

	rng := RangeIncluding(ten, twenty)

	// Move minimum to lower value.
	a.Equal(RangeIncluding(five, twenty), rng.WithMin(five))
	// Move minimum forward within range.
	a.Equal(RangeIncluding(ten.Next(), twenty), rng.WithMin(ten.Next()))
	// Set min to max, should create an empty range.
	a.Equal(RangeEmptyAt(twenty), rng.WithMin(twenty))
	// Set min to just beyond max, should create an empty range.
	a.Equal(RangeEmptyAt(twenty.Next()), rng.WithMin(twenty.Next()))
	// Set min to much larger value, should create an empty range.
	a.Equal(RangeEmptyAt(thirty), rng.WithMin(thirty))
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
		{"0.0000000000", true, 0, 0},
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
				bytes, err := actual.MarshalJSON()
				a.NoError(err)
				a.Equal([]byte(fmt.Sprintf("%q", test.testcase)), bytes)

				var unmarshaled Time
				a.NoError(json.Unmarshal(bytes, &unmarshaled))
				a.Equal(actual, unmarshaled)
			} else if !test.expectedPass {
				a.Error(actualErr)
			}
		})
	}
}

func TestSQLHelpers(t *testing.T) {
	r := require.New(t)

	now := New(100, 200)

	value, err := now.Value()
	r.NoError(err)
	r.Equal("100.0000000200", value)

	var next Time
	r.NoError(next.Scan(value))
	r.Equal(now, next)
}
