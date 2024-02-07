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

// Package lockset contains a utility type that orders access to
// multiple resources.
package lockset

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/util/notify"
)

// A Callback is provided to [Set.Schedule].
type Callback[K any] func(keys []K) error

// Status is returned by [Set.Schedule].
type Status struct {
	err error
}

// Sentinel instances of Status.
var (
	canceled  = &Status{err: context.Canceled}
	executing = &Status{}
	queued    = &Status{}
	success   = &Status{}
)

// Completed returns true if the callback has been called.
// See also [Status.Success].
func (s *Status) Completed() bool {
	return s == success || s.err != nil
}

// Err returns any error returned by the Callback.
func (s *Status) Err() error {
	return s.err
}

// Executing returns true if the Callback is currently executing.
func (s *Status) Executing() bool {
	return s == executing
}

// Queued returns true if the Callback has not been executed yet.
func (s *Status) Queued() bool {
	return s == queued
}

// Success returns true if the Status represents the successful
// completion of a scheduled waiter.
func (s *Status) Success() bool {
	return s == success
}

// A waiter represents a request to acquire locks on some number of
// keys. Instances of this type should only be accessed while
// holding the lock on the parent Set.
type waiter[K any] struct {
	fn     Callback[K] // nil if already executed.
	hoq    int         // The number of keys where this waiter is head of queue.
	keys   []K         // Desired key set.
	result notify.Var[*Status]
}

// Set implements an in-order admission queue for actors requiring
// exclusive access to a set of keys.
//
// A Set is internally synchronized and is safe for concurrent use. A
// zero-valued Set is ready to use. A Set should not be copied after it
// has been created.
type Set[K comparable] struct {
	mu struct {
		sync.Mutex
		// Deadlocks between waiters are avoided since the relative
		// order of enqueued waiters is maintained. That is, if
		// Schedule() is called with W1 and then W2, the first waiter
		// will be ahead of the second in all key queues that they have
		// in common. Furthermore, first waiter is guaranteed to be
		// executed, since it will be at the head of all its key queues.
		queues map[K][]*waiter[K]
	}
}

// Schedule executes the Callback once all keys have been locked.
// The result from the callback is available through the returned
// variable.
//
// It is valid to call this method with an empty key slice. The
// callback will simply be executed in a separate goroutine.
//
// The provided key slice will be deduplicated to avoid a callback
// from deadlocking with itself.
//
// Callbacks must not schedule new tasks and proceed to wait upon them.
// This will lead to deadlocks.
//
// The cancel function may be called to asynchronously dequeue and
// cancel the callback. If the callback has already started executing,
// the cancel callback will have no effect.
func (s *Set[K]) Schedule(keys []K, fn Callback[K]) (status *notify.Var[*Status], cancel func()) {
	// Make a copy of the key slice and deduplicate it.
	keys = append([]K(nil), keys...)
	seen := make(map[K]struct{}, len(keys))
	idx := 0
	for _, key := range keys {
		if _, dup := seen[key]; dup {
			continue
		}

		keys[idx] = key
		idx++
		seen[key] = struct{}{}
	}
	keys = keys[:idx]

	w := &waiter[K]{
		fn:   fn,
		keys: keys,
	}
	w.result.Set(queued)
	s.enqueue(w)
	return &w.result, func() {
		// Swap the callback so that it does nothing. We want to guard
		// against revivifying an already completed waiter, so we
		// look at whether a function is still defined.
		s.mu.Lock()
		needsDispose := w.fn != nil
		if needsDispose {
			w.fn = func([]K) error { return context.Canceled }
		}
		s.mu.Unlock()

		// Async cleanup.
		if needsDispose {
			s.dispose(w, true)
		}
	}
}

// dequeue removes the waiter from all wait queues. This method will
// also update the head of queue counts for any newly-eligible waiter.
// Waiters that have reached the heads of their respective queues will
// be returned so that they may be executed.
func (s *Set[K]) dequeue(w *waiter[K]) []*waiter[K] {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ret []*waiter[K]
	// Remove the waiter from each key's queue.
	for _, k := range w.keys {
		q := s.mu.queues[k]

		// Search for the waiter in the queue. It's always going to be
		// the first element in the slice, except in the cancellation
		// case.
		var idx int
		for idx = range q {
			if q[idx] == w {
				break
			}
		}
		if idx == len(q) {
			panic("waiter not found in queue")
		}

		// If the waiter was the first in the queue (likely), promote
		// the next waiter, possibly making it eligible to be run.
		if idx == 0 {
			q = q[1:]
			// The waiter was the only element of the queue, so we'll
			// just delete the slice from the map.
			if len(q) == 0 {
				delete(s.mu.queues, k)
				continue
			}

			// Promote the next waiter. If the waiter is now at the
			// head of its queues, return it so it can be started.
			head := q[0]
			head.hoq++
			if head.hoq == len(head.keys) {
				ret = append(ret, head)
			} else if head.hoq > len(head.keys) {
				panic("over counted")
			}
		} else {
			// The (canceled) waiter was in the middle of the queue,
			// just remove it from the slice.
			q = append(q[:idx], q[idx+1:]...)
		}

		// Put the shortened queue back in the map.
		s.mu.queues[k] = q
	}
	return ret
}

// dispose of the waiter callback in a separate goroutine. The waiter
// will be dequeued from the Set, possibly leading to cascading
// callbacks.
func (s *Set[K]) dispose(w *waiter[K], cancel bool) {
	go func() {
		// Clear the function reference to make the effects of dispose a
		// one-shot.
		s.mu.Lock()
		fn := w.fn
		w.fn = nil
		s.mu.Unlock()

		// Already executed and/or canceled.
		if fn == nil {
			return
		}

		// Once the waiter has been disposed of, dequeue it to release
		// its locks and dispose any unblocked waiters.
		defer func() {
			next := s.dequeue(w)
			for _, head := range next {
				s.dispose(head, false)
			}
		}()

		// If a cancellation is requested, set the variable.
		if cancel {
			w.result.Set(canceled)
			return
		}

		// Install panic handler before executing user code.
		defer func() {
			x := recover()
			switch t := x.(type) {
			case nil:
			// Success.
			case error:
				w.result.Set(&Status{err: t})
			default:
				w.result.Set(&Status{err: fmt.Errorf("panic in waiter: %v", t)})
			}
		}()

		w.result.Set(executing)
		err := fn(w.keys)
		if err == nil {
			w.result.Set(success)
		} else {
			w.result.Set(&Status{err: err})
		}
	}()
}

// enqueue adds the waiter to the Set. If the waiter is immediately
// eligible for execution, it will be started.
func (s *Set[K]) enqueue(w *waiter[K]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.queues == nil {
		s.mu.queues = make(map[K][]*waiter[K])
	}

	// Add the waiter to each queue. If it's the only waiter for that
	// key, also increment its hoq counter.
	for _, k := range w.keys {
		q := s.mu.queues[k]
		q = append(q, w)
		s.mu.queues[k] = q
		if len(q) == 1 {
			w.hoq++
		}
	}

	// This will also be satisfied if the waiter has an empty key set.
	if w.hoq == len(w.keys) {
		s.dispose(w, false)
	}
}
