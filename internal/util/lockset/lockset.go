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

// RetryAtHead returns an error that tasks can use to be retried later,
// once all preceding tasks have completed. If this error is returned
// when there are no preceding tasks, the causal error will be emitted
// from [Set.Schedule].
func RetryAtHead(cause error) *RetryAtHeadErr {
	return &RetryAtHeadErr{cause, nil}
}

// RetryAtHeadErr is returned by [RetryAtHead].
type RetryAtHeadErr struct {
	cause    error
	fallback func()
}

// Error returns a message.
func (e *RetryAtHeadErr) Error() string { return "callback requested a retry" }

// Or sets a fallback function to invoke if the task was already
// at the head of the global queue. This is used if a cleanup task
// must be run if the task is not going to be retried. The receiver
// is returned.
func (e *RetryAtHeadErr) Or(fn func()) *RetryAtHeadErr { e.fallback = fn; return e }

// Unwrap returns the causal error passed to [RetryAtHead].
func (e *RetryAtHeadErr) Unwrap() error { return e.cause }

// A Callback is provided to [Set.Schedule].
type Callback[K any] func(keys []K) error

// A Runner can be provided to Set to control the execution of scheduled
// tatks.
type Runner interface {
	// Go should execute the function in a non-blocking fashion.
	Go(func(context.Context)) error
}

// Status is returned by [Set.Schedule].
type Status struct {
	err error
}

// StatusFor constructs a successful status if err is null. Otherwise,
// it returns a new Status object that returns the error.
func StatusFor(err error) *Status {
	if err == nil {
		return success
	}
	return &Status{err: err}
}

// Outcome is a convenience type alias.
type Outcome = *notify.Var[*Status]

// NewOutcome is a convenience method to allocate an Outcome.
func NewOutcome() Outcome {
	return notify.VarOf(executing)
}

// Sentinel instances of Status.
var (
	executing      = &Status{}
	queued         = &Status{}
	retryQueued    = &Status{}
	retryRequested = &Status{}
	success        = &Status{}
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

// Retrying returns true if the callback returned [RetryAtHead] and it
// has not yet been re-attempted.
func (s *Status) Retrying() bool {
	return s == retryRequested || s == retryQueued
}

// Success returns true if the Status represents the successful
// completion of a scheduled waiter.
func (s *Status) Success() bool {
	return s == success
}

func (s *Status) String() string {
	switch s {
	case executing:
		return "executing"
	case queued:
		return "queued"
	case retryQueued:
		return "retryQueued"
	case retryRequested:
		return "retryRequested"
	case success:
		return "success"
	default:
		return "error: " + s.err.Error()
	}
}

// A waiter represents a request to acquire locks on some number of
// keys. Instances of this type should only be accessed while
// holding the lock on the parent Set.
type waiter[K any] struct {
	fn        Callback[K]         // nil if already executed.
	headCount int                 // The number of keys where this waiter is head of queue.
	keys      []K                 // Desired key set.
	next      *waiter[K]          // The waiter that was scheduled next.
	result    notify.Var[*Status] // The outbox for the waiter.
}

// Set implements an in-order admission queue for actors requiring
// exclusive access to a set of keys.
//
// A Set is internally synchronized and is safe for concurrent use. A
// zero-valued Set is ready to use. A Set should not be copied after it
// has been created.
type Set[K comparable] struct {
	// If non-nil, execute goroutines through this [Runner] instead of
	// using the go keyword. This allows callers to provide their own
	// concurrency control. If the [Runner] rejects a scheduled task,
	// the rejection will be available from the [notify.Var] returned
	// from [Set.Schedule].
	Runner Runner

	mu struct {
		sync.Mutex
		// These waiters are used to maintain a global ordering of
		// waiters to implement [RetryAtHead].
		head, tail *waiter[K]

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
// Callbacks that need to be retried may return [RetryAtHead]. This will
// execute the callback again when all other callbacks scheduled before
// it have been completed. A retrying callback will continue to hold its
// key locks until the retry has taken place.
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
func (s *Set[K]) Schedule(keys []K, fn Callback[K]) (outcome Outcome, cancel func()) {
	keys = dedup(keys)

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
	status, _ := w.result.Get()

	// If the waiter has reached a terminal condition, clean up its
	// entries.
	if status.Completed() {
		// Remove the waiter from each key's queue.
		for _, k := range w.keys {
			q := s.mu.queues[k]

			// Search for the waiter in the queue. It's always going to
			// be the first element in the slice, except in the
			// cancellation case.
			var idx int
			for idx = range q {
				if q[idx] == w {
					break
				}
			}

			if idx == len(q) {
				panic(fmt.Sprintf("waiter not found in queue: %d", idx))
			}

			// If the waiter was the first in the queue (likely),
			// promote the next waiter, possibly making it eligible to
			// be run.
			if idx == 0 {
				q = q[1:]
				if len(q) == 0 {
					// The waiter was the only element of the queue, so
					// we'll just delete the slice from the map.
					delete(s.mu.queues, k)
					continue
				}

				// Promote the next waiter. If the waiter is now at the
				// head of its queues, it can be started.
				head := q[0]
				head.headCount++
				if head.headCount == len(head.keys) {
					ret = append(ret, head)
				} else if head.headCount > len(head.keys) {
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
	}

	// Make some progress on the global queue.
	head := s.mu.head
	for head != nil {
		outcome, _ := head.result.Get()

		// Advance the queue if the head waiter is finished.
		if outcome.Completed() {
			head = head.next
			s.mu.head = head
			continue
		}

		// The head has requested to be retried, so add it to the
		// slice of waiters to execute. Changing the status here
		// also ensures that this action is a one-shot.
		if outcome == retryRequested {
			head.result.Set(retryQueued)
			ret = append(ret, head)
		}
		break
	}
	// If we reached the end, clear the tail field.
	if head == nil {
		s.mu.tail = nil
	}

	return ret
}

// dispose of the waiter callback in a separate goroutine. The waiter
// will be dequeued from the Set, possibly leading to cascading
// callbacks.
func (s *Set[K]) dispose(w *waiter[K], cancel bool) {
	work := func(_ context.Context) {
		// Clear the function reference to make the effects of dispose a
		// one-shot.
		s.mu.Lock()
		fn := w.fn
		w.fn = nil
		startedAtHead := w == s.mu.head
		s.mu.Unlock()

		// Already executed and/or canceled.
		if fn == nil {
			return
		}

		// Set canceled status or execute the callback.
		var err error
		if cancel {
			err = context.Canceled
		} else {
			w.result.Set(executing)
			err = tryCall(fn, w.keys)
		}

		// Once the waiter has been called, update its status and call
		// dequeue to find any tasks that have been unblocked.
		switch t := err.(type) {
		case nil:
			w.result.Set(success)

		case *RetryAtHeadErr:
			// The callback requested to be retried later.
			if startedAtHead {
				// The waiter was already executing at the global head
				// of the queue. Reject the request and execute any
				// fallback handler that may have been provided.
				if t.fallback != nil {
					t.fallback()
				}
				retryErr := t.Unwrap()
				if retryErr == nil {
					w.result.Set(success)
				} else {
					w.result.Set(&Status{err: retryErr})
				}
			} else {
				// Otherwise, re-enable the waiter. The status will be
				// set to retryRequested for later re-dispatching by the
				// dispose method.
				s.mu.Lock()
				w.fn = fn
				w.result.Set(retryRequested)
				endedAtHead := w == s.mu.head
				s.mu.Unlock()

				// It's possible that another task completed while this
				// one was executing, which moved it to the head of the
				// global queue. If this happens, we need to immediately
				// queue up its retry.
				if !startedAtHead && endedAtHead {
					s.dispose(w, false)
				}

				// We can't dequeue the waiter if it's going to retry
				// later on. That would incorrectly unblock anything
				// also waiting on this waiter's keys.
				return
			}
		default:
			w.result.Set(&Status{err: err})
		}

		// Remove the waiter's locks and get a slice of new tasks to
		// kick off.
		next := s.dequeue(w)
		for _, unblocked := range next {
			s.dispose(unblocked, false)
		}
	}

	if s.Runner == nil {
		go work(context.Background())
		return
	}

	if err := s.Runner.Go(work); err != nil {
		w.result.Set(&Status{err: err})
	}
}

// enqueue adds the waiter to the Set. If the waiter is immediately
// eligible for execution, it will be started.
func (s *Set[K]) enqueue(w *waiter[K]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.queues == nil {
		s.mu.queues = make(map[K][]*waiter[K])
	}

	// Insert the waiter into the global queue.
	if s.mu.tail == nil {
		s.mu.head = w
	} else {
		s.mu.tail.next = w
	}
	s.mu.tail = w

	// Add the waiter to each key queue. If it's the only waiter for
	// that key, also increment its headCount.
	for _, k := range w.keys {
		q := s.mu.queues[k]
		q = append(q, w)
		s.mu.queues[k] = q
		if len(q) == 1 {
			w.headCount++
		}
	}

	// This will also be satisfied if the waiter has an empty key set.
	if w.headCount == len(w.keys) {
		s.dispose(w, false)
	}
}

// Wait returns the first non-nil error.
func Wait(ctx context.Context, outcomes []Outcome) error {
outcome:
	for _, outcome := range outcomes {
		for {
			status, changed := outcome.Get()
			if status.Success() {
				continue outcome
			}
			if err := status.Err(); err != nil {
				return err
			}
			select {
			case <-changed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// Make a copy of the key slice and deduplicate it.
func dedup[K comparable](keys []K) []K {
	keys = append([]K(nil), keys...)
	seen := make(map[K]struct{}, len(keys))
	idx := 0
	for _, key := range keys {
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		keys[idx] = key
		idx++
	}
	keys = keys[:idx]
	return keys
}

// tryCall invokes the function with a panic handler.
func tryCall[K any](fn func([]K) error, keys []K) (err error) {
	// Install panic handler before executing user code.
	defer func() {
		x := recover()
		switch t := x.(type) {
		case nil:
		// Success.
		case error:
			err = t
		default:
			err = fmt.Errorf("panic in waiter: %v", t)
		}
	}()

	return fn(keys)
}
