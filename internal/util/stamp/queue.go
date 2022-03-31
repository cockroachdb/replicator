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
	"container/list"

	"github.com/pkg/errors"
)

// Queue maintains ordered Stamped values.
//
// Draining values from the queue advances a "consistent point", derived
// from the stamps of the dequeued values and a queue of candidate,
// "marked" stamps. The lowest-valued marked stamp becomes the
// consistent point when there are no more stamped values in the queue
// whose stamps are less than the mark.
//
// Queue is not internally synchronized. The zero value is
// ready to use. A Queue should not be copied once created.
//
// At present, this implementation assumes that it is being driven from
// a serial source of data that provides monotonic stamps. It would be
// possible to extend the behavior to allow for out-of-order insertion,
// albeit with increased algorithmic complexity, provided that the stamp
// of the enqueued data is still greater than the consistent point.
//
// The following Stamp-ordering invariants are maintained:
//   1) enqueued[0] <= enqueued[1] ...  <= enqueued[n] (softly monotonic)
//   2) marked[0] < marked[1] ... < marked[n] (strictly monotonic)
//   3) consistent < enqueued[0]
//   4) consistent < marked[0]
//   5) consistent > previousConsistent.
type Queue struct {
	noCopy

	consistent Stamp
	enqueued   list.List // Stamped values.
	marked     list.List // Stamp values. Strictly monotonic.
}

// Consistent returns a Stamp which is less than all enqueued or marked
// stamps. This value will be nil if none of Drain, Mark, or
// setConsistent have been called.
func (s *Queue) Consistent() Stamp {
	return s.consistent
}

// Drain returns the next enqueued, Stamped value, or nil if the Queue
// is empty. Draining a value will advance the consistent stamp to the
// greatest marked value that is strictly less than the Stamp of the
// value returned.  That is, the consistent stamp will be in the
// half-open range of [ nil, Drain().Stamp() ).
func (s *Queue) Drain() Stamped {
	front := s.enqueued.Front()
	if front == nil {
		return nil
	}

	ret := s.enqueued.Remove(front).(Stamped)
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

	if tail := s.enqueued.Back(); tail != nil {
		if c, ok := tail.Value.(Coalescing); ok {
			next = c.Coalesce(next)
			if next == nil {
				return nil
			}
		}
	}

	added := s.enqueued.PushBack(next)
	if err := s.validate(); err != nil {
		s.enqueued.Remove(added)
		return err
	}
	return nil
}

// Enqueued returns all currently-enqueued values. The returned slice
// may be modified by the caller.
func (s *Queue) Enqueued() []Stamped {
	elt := s.enqueued.Front()
	ret := make([]Stamped, s.enqueued.Len())
	for idx := range ret {
		ret[idx] = elt.Value.(Stamped)
		elt = elt.Next()
	}
	return ret
}

// Mark schedules the marked Stamp to be made consistent. This method
// will update the consistent point if the provided stamp is greater
// than the current consistent point and less than the stamp of the head
// of the queue or if the queue is empty.
func (s *Queue) Mark(next Stamp) error {
	elt := s.marked.PushBack(next)
	if err := s.validate(); err != nil {
		s.marked.Remove(elt)
		return err
	}
	return s.advanceConsistentPoint()
}

// Marked returns all currently-marked values. The returned slice
// may be modified by the caller.
func (s *Queue) Marked() []Stamp {
	elt := s.marked.Front()
	ret := make([]Stamp, s.marked.Len())
	for idx := range ret {
		ret[idx] = elt.Value.(Stamp)
		elt = elt.Next()
	}
	return ret
}

// PeekMarked returns the head of the marked queue, without modifying it.
func (s *Queue) PeekMarked() Stamp {
	if elt := s.marked.Front(); elt != nil {
		return elt.Value.(Stamp)
	}
	return nil
}

// PeekQueued returns the head of the queue, without modifying it.
func (s *Queue) PeekQueued() Stamped {
	if elt := s.enqueued.Front(); elt != nil {
		return elt.Value.(Stamped)
	}
	return nil
}

// advanceConsistentPoint updates the consistent point to the greatest
// marked value that is less than the queue-head stamp.
func (s *Queue) advanceConsistentPoint() error {
	var nextConsistent Stamp

	if queuedHead := s.enqueued.Front(); queuedHead == nil {
		// Nothing queued, set the next consistent stamp to the tail
		// (greatest) marked value and reset the marked list.
		if tail := s.marked.Back(); tail != nil {
			nextConsistent = tail.Value.(Stamp)
			s.marked.Init()
		}
	} else {
		// Remove elements from the head of the marked list that are
		// strictly less than the stamp of the value at the beginning of
		// the queued data.
		queuedHeadStamp := queuedHead.Value.(Stamped).Stamp()
		for {
			if markHead := s.marked.Front(); markHead != nil {
				mark := markHead.Value.(Stamp)
				if mark.Less(queuedHeadStamp) {
					nextConsistent = mark
					s.marked.Remove(markHead)
					continue
				}
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
	// Invariant 1: enqueued[0] <= enqueued[1] ...  <= enqueued[idx]
	// Check only the last two elements of the enqueued list, since
	// we're only ever appending.
	if b := s.enqueued.Back(); b != nil {
		bStamp := b.Value.(Stamped).Stamp()
		if a := b.Prev(); a != nil {
			aStamp := a.Value.(Stamped).Stamp()
			if Compare(bStamp, aStamp) < 0 {
				idx := s.enqueued.Len() - 1
				return errors.Errorf(
					"enqueued[%d] %s must be >= than enqueued[%d] %s",
					idx, bStamp, idx-1, aStamp)
			}
		}
	}

	// Invariant 2: marked[0] < marked[1] ...  < marked[idx]
	// Check only the last two elements of the enqueued list, since
	// we're only ever appending.
	if b := s.marked.Back(); b != nil {
		bStamp := b.Value.(Stamp)
		if a := b.Prev(); a != nil {
			aStamp := a.Value.(Stamp)
			if Compare(bStamp, aStamp) <= 0 {
				idx := s.enqueued.Len() - 1
				return errors.Errorf(
					"enqueued[%d] %s must be strictly greater than enqueued[%d] %s",
					idx, bStamp, idx-1, aStamp)
			}
		}
	}

	// Invariant 3: consistent < enqueued[0]
	if queued := s.PeekQueued(); queued != nil {
		if Compare(s.consistent, queued.Stamp()) >= 0 {
			return errors.Errorf(
				"consistent stamp %s must be strictly less than first enqueued stamp %s",
				s.consistent, queued.Stamp())
		}
	}

	// Invariant 4: consistent < marked[0]
	if mark := s.PeekMarked(); mark != nil {
		if Compare(s.consistent, mark) >= 0 {
			return errors.Errorf("consistent stamp %s > marked stamp %s", s.consistent, mark)
		}
	}

	return nil
}
