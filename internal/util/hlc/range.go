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

import "fmt"

// Range represents a half-open range of HLC values, inclusive of Min
// and exclusive of Max. For code readability, prefer using [RangeEmpty]
// or [RangeIncluding] to construct ranges instead of directly creating
// a [Range].
type Range [2]Time

// RangeEmpty returns an empty range.
func RangeEmpty() Range {
	return Range{}
}

// RangeEmptyAt returns a Range that starts at the given time, but for
// which [Range.Empty] will return true.
func RangeEmptyAt(ts Time) Range {
	return Range{ts, ts}
}

// RangeExcluding returns a Range that includes the start time and
// excludes the end time.
func RangeExcluding(startInclusive, endExclusive Time) Range {
	return Range{startInclusive, endExclusive}
}

// RangeIncluding returns the smallest range that includes both the
// start and end times.
func RangeIncluding(start, end Time) Range {
	return Range{start, end.Next()}
}

// Empty returns true if the Min time is greater than or equal to the
// Max value.
func (r Range) Empty() bool { return Compare(r[0], r[1]) >= 0 }

// Min returns the inclusive, minimum value.
func (r Range) Min() Time { return r[0] }

// Max returns the exclusive, maximum value.
func (r Range) Max() Time { return r[1] }

// MaxInclusive returns the maximum value that is within the range. That
// is, it returns one tick before the exclusive end bound.
func (r Range) MaxInclusive() Time { return r[1].Before() }

func (r Range) String() string {
	return fmt.Sprintf("[ %s -> %s )", r[0], r[1])
}

// WithMin returns a new range that includes the minimum. If the new
// minimum is equal to or beyond the end of the range, this is
// equivalent to calling [RangeEmptyAt].
func (r Range) WithMin(min Time) Range {
	if Compare(min, r.Max().Before()) >= 0 {
		return RangeEmptyAt(min)
	}
	return Range{min, r[1]}
}
