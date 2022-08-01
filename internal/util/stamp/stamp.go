// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
