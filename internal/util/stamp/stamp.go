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

// Package stamp contains a utility for maintaining certain ordering
// invariants when queuing (time-)stamped values.
package stamp

import "sync"

// A Stamp is a comparable value (possibly a time, or an offset within a
// log file). Stamp implementations will be marshaled using the json
// package.
type Stamp interface {
	// Less returns true if the callee should be sorted before the other
	// Stamp.
	Less(other Stamp) bool
}

// Compare returns -1, 0, or 1 if a is less than, equal to, or
// greater than b. This function is nil-safe; a nil Stamp is less than
// any non-nil Stamp.
func Compare(a, b Stamp) int {
	switch {
	// Identity or nil-nil case.
	case a == b:
		return 0
	case a == nil, a == Stamp(nil):
		return -1
	case b == nil, b == Stamp(nil):
		return 1
	case a.Less(b):
		return -1
	case b.Less(a):
		return 1
	default:
		return 0
	}
}

// A Stamped value has some key by which it can be ordered.
type Stamped interface {
	// Stamp returns the Stamp associated with the value.
	Stamp() Stamp
}

// Coalescing is an optional interface that may be implemented by
// Stamped values.
type Coalescing interface {
	Stamped

	// Coalesce may be implemented to allow the tail node in the queue
	// to be coalesced with some proposed value to be added to the end
	// of the queue. Implementations should modify the callee as
	// appropriate to append the next tail value. This method can return
	// an alternate, non-nil value to still be added to the queue. If
	// this method returns nil, then the tail value will be considered
	// to have been fully coalesced and no new value will be added to
	// the queue.
	Coalesce(tail Stamped) (remainder Stamped)
}

// Tie into go vet static check for copying a Locker instance.
type noCopy struct{}

var _ sync.Locker = (*noCopy)(nil)

func (c *noCopy) Lock()   {}
func (c *noCopy) Unlock() {}
