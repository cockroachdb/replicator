// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stamp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	a := assert.New(t)
	var q Queue
	a.Equal([]Stamped{}, q.Enqueued())
	a.Nil(q.Consistent())

	// Verify that marking an empty queue advances the consistent point.
	a.NoError(q.Mark(intStamp(-1)))
	a.Equal(intStamp(-1), q.Consistent())

	// Verify that Mark is strictly monotonic.
	a.Error(q.Mark(intStamp(-1)))

	// Check setConsistent is strictly monotonic.
	a.NoError(q.setConsistent(intStamp(0)))
	a.Equal(intStamp(0), q.Consistent())
	a.Error(q.setConsistent(intStamp(0)))
	a.Error(q.setConsistent(intStamp(-1)))
	a.Equal(intStamp(0), q.Consistent())

	// Enqueue duplicate values, which will be coalesced by intStamp.
	a.NoError(q.Enqueue(intStamp(1)))
	a.NoError(q.Mark(intStamp(1)))

	a.NoError(q.Enqueue(intStamp(2)))
	a.NoError(q.Enqueue(intStamp(2)))
	a.NoError(q.Mark(intStamp(2)))

	a.NoError(q.Enqueue(intStamp(3)))
	a.NoError(q.Enqueue(intStamp(3)))
	a.NoError(q.Enqueue(intStamp(3)))
	a.NoError(q.Mark(intStamp(3)))

	a.Equal([]Stamped{intStamp(1), intStamp(2), intStamp(3)}, q.Enqueued())
	a.Equal([]Stamp{intStamp(1), intStamp(2), intStamp(3)}, q.Marked())

	// Make sure enqueued can't go backwards.
	a.Error(q.Enqueue(intStamp(1)))
	a.Len(q.Enqueued(), 3)

	// Make sure marked can't go backwards.
	a.Error(q.Mark(intStamp(1)))
	a.Len(q.Enqueued(), 3)

	// Make sure that consistent is always less than first enqueued.
	a.Error(q.setConsistent(intStamp(1)))
	a.Error(q.setConsistent(intStamp(2)))
	a.Error(q.setConsistent(intStamp(3)))
	a.Error(q.setConsistent(intStamp(100)))

	a.Equal(intStamp(1), q.PeekQueued())
	a.Equal(intStamp(1), q.PeekMarked())
	a.Len(q.Enqueued(), 3)
	a.Len(q.Marked(), 3)

	// Pull values out, verify that consistent point also increases.
	a.Equal(intStamp(1), q.Drain())
	a.Equal(intStamp(1), q.Consistent())
	a.Equal(intStamp(2), q.Drain())
	a.Equal(intStamp(2), q.Consistent())
	a.Equal(intStamp(3), q.Drain())
	a.Equal(intStamp(3), q.Consistent())
	a.Nil(q.Drain())
	a.Nil(q.PeekQueued())
	a.Nil(q.PeekMarked())
	a.Empty(q.Enqueued())

	// Add and drain some additional values, but don't expect the
	// consistent point to advance until we add a mark.
	a.NoError(q.Enqueue(intStamp(4)))
	a.NoError(q.Enqueue(intStamp(5)))
	a.Equal(intStamp(4), q.Drain())
	a.Equal(intStamp(5), q.Drain())
	a.Equal(intStamp(3), q.Consistent())
	a.Nil(q.PeekMarked())

	a.NoError(q.Mark(intStamp(5)))
	a.Equal(intStamp(5), q.Consistent())

	a.Equal([]Stamped{}, q.Enqueued())
	a.Equal([]Stamp{}, q.Marked())

	// Make sure consistent point can't be set backwards.
	a.Error(q.setConsistent(intStamp(1)))

	// Make sure mark can't be set below consistent point.
	a.Error(q.Mark(intStamp(1)))
}

func TestQueueDrainPanic(t *testing.T) {
	a := assert.New(t)
	var q Queue

	a.NoError(q.Enqueue(intStamp(1)))
	a.NoError(q.Enqueue(intStamp(2)))
	a.NoError(q.Mark(intStamp(1)))

	// Violate invariant by making consistent > marked.
	q.consistent = intStamp(3)

	a.Panics(func() { q.Drain() })
}
