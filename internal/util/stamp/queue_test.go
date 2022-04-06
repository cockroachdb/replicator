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
	a.Empty(q.Values(nil))
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

	a.Equal([]Stamped{intStamp(1), intStamp(2), intStamp(3)}, q.Values(nil))
	a.Equal([]Stamp{intStamp(1), intStamp(2), intStamp(3)}, q.Markers(nil))

	// Make sure values can't go backwards.
	a.Error(q.Enqueue(intStamp(1)))
	a.Len(q.Values(nil), 3)

	// Make sure markers can't go backwards.
	a.Error(q.Mark(intStamp(1)))
	a.Len(q.Values(nil), 3)

	// Make sure that consistent is always less than first values.
	a.Error(q.setConsistent(intStamp(1)))
	a.Error(q.setConsistent(intStamp(2)))
	a.Error(q.setConsistent(intStamp(3)))
	a.Error(q.setConsistent(intStamp(100)))

	a.Equal(intStamp(1), q.Peek())
	a.Equal(intStamp(1), q.PeekMarker())
	a.Len(q.Values(nil), 3)
	a.Len(q.Markers(nil), 3)

	// Pull values out, verify that consistent point also increases.
	a.Equal(intStamp(1), q.Dequeue())
	a.Equal(intStamp(1), q.Consistent())
	a.Equal(intStamp(2), q.Dequeue())
	a.Equal(intStamp(2), q.Consistent())
	a.Equal(intStamp(3), q.Dequeue())
	a.Equal(intStamp(3), q.Consistent())
	a.Nil(q.Dequeue())
	a.Nil(q.Peek())
	a.Nil(q.PeekMarker())
	a.Empty(q.Values(nil))

	// Add and drain some additional values, but don't expect the
	// consistent point to advance until we add a mark.
	a.NoError(q.Enqueue(intStamp(4)))
	a.NoError(q.Enqueue(intStamp(5)))
	a.Equal(intStamp(4), q.Dequeue())
	a.Equal(intStamp(5), q.Dequeue())
	a.Equal(intStamp(3), q.Consistent())
	a.Nil(q.PeekMarker())

	a.NoError(q.Mark(intStamp(5)))
	a.Equal(intStamp(5), q.Consistent())

	a.Empty(q.Values(nil))
	a.Empty(q.Markers(nil))

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

	// Violate invariant by making consistent > markers.
	q.consistent = intStamp(3)

	a.Panics(func() { q.Dequeue() })
}
