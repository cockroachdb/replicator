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

package stamp

import (
	"container/list"

	"github.com/pkg/errors"
)

// Queue maintains ordered Stamped values.
//
// Draining values from the queue advances a "consistent point", derived
// from the stamps of the dequeued values and a queue of candidate,
// "marker" stamps. The lowest-valued marker becomes the consistent
// point when there are no more stamped values in the queue whose stamps
// are less than the marker.
//
// Queue is not internally synchronized. The zero value is
// ready to use. A Queue should not be copied once created.
//
// At present, this implementation assumes that it is being driven from
// a serial source of data that provides monotonic stamps. It would be
// possible to extend the behavior to allow for out-of-order insertion,
// albeit with increased algorithmic complexity, provided that the stamp
// of the enqueued value is still greater than the consistent point.
//
// The following Stamp-ordering invariants are maintained:
//  1. values[0] <= values[1] ...  <= values[n] (softly monotonic)
//  2. markers[0] < markers[1] ... < markers[n] (strictly monotonic)
//  3. consistent < values[0]
//  4. consistent < markers[0]
//  5. consistent > previousConsistent.
type Queue struct {
	consistent Stamp
	markers    list.List // Stamp values. Strictly monotonic.
	values     list.List // Stamped values. Softly monotonic.

	// Prevent Lock() methods from escaping into public API.
	_ struct {
		noCopy
	}
}

// Consistent returns a Stamp which is less than the stamps of all
// enqueued values or markers. This value will be nil if none of
// Dequeue, Mark, or setConsistent have been called.
func (s *Queue) Consistent() Stamp {
	return s.consistent
}

// Dequeue returns the next Stamped value, or nil if the Queue is empty.
// Draining a value will advance the consistent point to the greatest
// marker that is strictly less than the Stamp of the value
// returned.  That is, the consistent Stamp will be in the half-open
// range of [ nil, Dequeue().Stamp() ).
func (s *Queue) Dequeue() Stamped {
	front := s.values.Front()
	if front == nil {
		return nil
	}

	ret := s.values.Remove(front).(Stamped)
	if err := s.advanceConsistentPoint(); err != nil {
		// Panic here, since this should only fail if invariants
		// are not correctly maintained elsewhere in the code.
		panic(err)
	}

	return ret
}

// Enqueue adds a Stamped value to the end of the queue. If the tail
// value in the Queue implements Coalescing, the Queue will attempt to
// merge the tail and next values.
func (s *Queue) Enqueue(next Stamped) error {
	// To support out-of-order behavior, this method would need to
	// (reverse-)scan the queue to find the correct insertion point.
	// Another option is to switch the fields from lists to min-heaps.

	// Try to coalesce with the tail value.
	if tailElt := s.values.Back(); tailElt != nil {
		if c, ok := tailElt.Value.(Coalescing); ok {
			// Let the Coalesce method replace the value to enqueue.
			// If there is no "remainder" value to add to the queue,
			// just return here.
			next = c.Coalesce(next)
			if next == nil {
				return nil
			}
		}
	}

	added := s.values.PushBack(next)
	if err := s.validate(); err != nil {
		s.values.Remove(added)
		return err
	}
	return nil
}

// Mark adds a potential future consistent point to the Queue. This
// method will update the consistent point if the new marker is greater
// than the current consistent point and less than the stamp of the head
// of the queue or if the queue is empty.
func (s *Queue) Mark(marker Stamp) error {
	elt := s.markers.PushBack(marker)
	if err := s.validate(); err != nil {
		s.markers.Remove(elt)
		return err
	}
	return s.advanceConsistentPoint()
}

// Markers appends all markers passed to Mark that are greater than the
// current consistent point. The modified slice is returned.
func (s *Queue) Markers(buf []Stamp) []Stamp {
	for elt := s.markers.Front(); elt != nil; elt = elt.Next() {
		buf = append(buf, elt.Value.(Stamp))
	}
	return buf
}

// Peek returns the value at the head of the queue, without modifying it.
func (s *Queue) Peek() Stamped {
	if elt := s.values.Front(); elt != nil {
		return elt.Value.(Stamped)
	}
	return nil
}

// PeekMarker returns the head of the markers queue, without modifying it.
func (s *Queue) PeekMarker() Stamp {
	if elt := s.markers.Front(); elt != nil {
		return elt.Value.(Stamp)
	}
	return nil
}

// Values appends all Stamped values in the Queue. The modified slice is
// returned.
func (s *Queue) Values(buf []Stamped) []Stamped {
	for elt := s.values.Front(); elt != nil; elt = elt.Next() {
		buf = append(buf, elt.Value.(Stamped))
	}
	return buf
}

// advanceConsistentPoint updates the consistent point to the greatest
// marker value that is less than the stamp of the first value.
func (s *Queue) advanceConsistentPoint() error {
	var nextConsistent Stamp

	if vHeadElt := s.values.Front(); vHeadElt == nil {
		// Nothing queued, set the next consistent stamp to the tail
		// (greatest) marker and reset the markers list.
		if mTail := s.markers.Back(); mTail != nil {
			nextConsistent = mTail.Value.(Stamp)
			s.markers.Init()
		}
	} else {
		// Remove elements from the head of the markers list that are
		// strictly less than the stamp of the first value.
		vHeadStamp := vHeadElt.Value.(Stamped).Stamp()
		for mHeadElt := s.markers.Front(); mHeadElt != nil; mHeadElt = s.markers.Front() {
			marker := mHeadElt.Value.(Stamp)
			if marker.Less(vHeadStamp) {
				nextConsistent = marker
				s.markers.Remove(mHeadElt)
				continue
			}
			break
		}
	}

	if nextConsistent == nil {
		return nil
	}
	return s.setConsistent(nextConsistent)
}

// setConsistent allows the consistent point to be seeded or updated
// to any value which satisfies the order invariants.
func (s *Queue) setConsistent(next Stamp) error {
	prev := s.consistent

	// Invariant 5: consistent > previousConsistent
	if Compare(next, prev) <= 0 {
		return errors.Errorf("consistent stamp %s must always increase %s", next, prev)
	}

	s.consistent = next
	if err := s.validate(); err != nil {
		s.consistent = prev
		return err
	}
	return nil
}

// validate enforces the invariants.
// TODO(bob): Pass some kind of mutation flag to cut down on unnecessary tests.
func (s *Queue) validate() error {
	// Invariant 1: values[0] <= values[1] ...  <= values[idx]
	// Check only the last two elements of the values list, since
	// we're only ever appending.
	if b := s.values.Back(); b != nil {
		bStamp := b.Value.(Stamped).Stamp()
		if a := b.Prev(); a != nil {
			aStamp := a.Value.(Stamped).Stamp()
			if Compare(bStamp, aStamp) < 0 {
				idx := s.values.Len() - 1
				return errors.Errorf(
					"values[%d] %s must be >= than values[%d] %s",
					idx, bStamp, idx-1, aStamp)
			}
		}
	}

	// Invariant 2: markers[0] < markers[1] ...  < markers[idx]
	// Check only the last two elements of the values list, since
	// we're only ever appending.
	if b := s.markers.Back(); b != nil {
		bStamp := b.Value.(Stamp)
		if a := b.Prev(); a != nil {
			aStamp := a.Value.(Stamp)
			if Compare(bStamp, aStamp) <= 0 {
				idx := s.values.Len() - 1
				return errors.Errorf(
					"values[%d] %s must be strictly greater than values[%d] %s",
					idx, bStamp, idx-1, aStamp)
			}
		}
	}

	// Invariant 3: consistent < values[0]
	if queued := s.Peek(); queued != nil {
		if Compare(s.consistent, queued.Stamp()) >= 0 {
			return errors.Errorf(
				"consistent stamp %s must be strictly less than first values stamp %s",
				s.consistent, queued.Stamp())
		}
	}

	// Invariant 4: consistent < markers[0]
	if mark := s.PeekMarker(); mark != nil {
		if Compare(s.consistent, mark) >= 0 {
			return errors.Errorf("consistent stamp %s >= markers stamp %s", s.consistent, mark)
		}
	}

	return nil
}
