// Copyright 2024 The Cockroach Authors
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClock(t *testing.T) {
	r := require.New(t)

	wall := int64(100)
	c := &Clock{
		Wall: func() int64 { return wall },
	}

	last := c.Logical(0)
	r.Equal(wall, last.Nanos())
	r.Equal(0, last.Logical())

	advanced := c.Advance(last)
	r.True(Compare(advanced, last) > 0)

	now := c.Now()
	r.True(Compare(now, advanced) > 0)

	logical := 2000
	for i := range 100 {
		next := c.Logical(logical)

		// Time must always be monotonic.
		r.True(Compare(next, last) > 0)

		// Logical value must always match.
		r.Equal(logical, next.Logical())

		// Nanos must be non-zero and may run ahead of our wall time.
		r.GreaterOrEqual(next.Nanos(), int64(0))
		r.GreaterOrEqual(next.Nanos(), wall)

		last = next

		// Time passes...
		switch i % 8 {
		case 0:
			// Logical ticks.
			logical++

		case 1:
			// Wall time ticks.
			wall++

		case 2:
			// Wall and logical tick.
			logical++
			wall++

		case 3:
			// Logical wraps.
			logical -= 2

		case 4:
			// Wall goes backwards.
			wall -= 2

		case 5:
			// Wall and logical both go backwards.
			wall -= 2
			logical -= 2

		case 6:
			// Nothing changes.
			wall = last.Nanos()
			logical = last.Logical()

		case 7:
			// Within timer resolution.
			wall = last.Nanos()
			logical = last.Logical() + 1
		}
	}
	r.Equal(New(178, 1990), last)
	r.Equal(last, c.Last())

	// Force the clock to a specific value.
	c.Reset(New(100, 100))
	r.Equal(New(100, 100), c.Last())

	// Check External function.
	one := 1
	ext := c.External(&one)
	r.Same(&one, ext.External())

	// Ensure relative times retain ext reference.
	r.Same(&one, ext.Before().External())
	r.Same(&one, ext.Next().External())
}
