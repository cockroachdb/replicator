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
	"sync"
	"time"
)

// A Clock generates a monotonic sequence of [Time] values, based on
// some external logical clock and an approximate local wall time.
//
// The zero value is ready for use. A Clock is internally synchronized
// and is safe for use by multiple goroutines.
type Clock struct {
	Wall func() int64 // Override wall source for testing.

	mu struct {
		sync.Mutex
		last Time
	}
}

// Advance returns a monotonic [Time] value that is equal to or greater
// than the supplied timestamp.
func (c *Clock) Advance(ts Time) Time {
	return c.tick(ts.Nanos(), ts.Logical(), ts.External())
}

// External returns a monotonic [Time] value that will return the
// argument via [Time.External]. Note that the external data is not
// serialized by the Time type, nor does it affect comparisons between
// Time values.
//
// This method is useful for logical frontends whose progress is
// expressed via non-trivial datastructures (e.g. GTIDSet).
func (c *Clock) External(data any) Time {
	var proposedNanos int64
	if c.Wall != nil {
		proposedNanos = c.Wall()
	} else {
		proposedNanos = time.Now().UnixNano()
	}
	return c.tick(proposedNanos, 0, data)
}

// Last returns the most recently emitted Time.
func (c *Clock) Last() Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.last
}

// Logical returns a monotonic [Time] value based on an approximate wall
// time and a supplied logical offset.
func (c *Clock) Logical(logical int) Time {
	var proposedNanos int64
	if c.Wall != nil {
		proposedNanos = c.Wall()
	} else {
		proposedNanos = time.Now().UnixNano()
	}
	return c.tick(proposedNanos, logical, nil)
}

// Now returns a monotonic [Time] value based on approximate wall time.
func (c *Clock) Now() Time {
	return c.Logical(0)
}

// Reset the clock to a specific time. This method does not preserve
// monotonicity and should only be used to initialize a Clock.
func (c *Clock) Reset(ts Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.last = ts
}

func (c *Clock) tick(proposedNanos int64, logical int, ext any) Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	last := c.mu.last
	var nextNanos int64
	var nextLogical int
	if (proposedNanos > last.Nanos()) ||
		(proposedNanos == last.Nanos() && logical > last.Logical()) {
		// The general case is that the wall time should advance or that
		// we're within the wall time's resolution and the logical
		// component has advanced.
		nextNanos = proposedNanos
		nextLogical = logical
	} else {
		// The clock and/or logical values have gone backwards.
		nextNanos = last.Nanos() + 1
		nextLogical = logical
	}
	ret := Time{nextNanos, nextLogical, ext}
	c.mu.last = ret
	return ret
}
