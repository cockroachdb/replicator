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
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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

// A Runner is passed to New to begin the execution of tasks.
type Runner interface {
	// Go should execute the function in a non-blocking fashion.
	Go(func(context.Context)) error
}

// GoRunner returns a Runner that executes tasks using the go keyword
// and the specified context.
func GoRunner(ctx context.Context) Runner { return &goRunner{ctx} }

type goRunner struct {
	ctx context.Context
}

func (r *goRunner) Go(fn func(context.Context)) error {
	go fn(r.ctx)
	return nil
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
// holding the parent [Set.waiterMu] lock.
type waiter[K any] struct {
	fn            Callback[K]         // nil if already executed.
	keys          []K                 // Desired key set.
	result        notify.Var[*Status] // The outbox for the waiter.
	scheduleStart time.Time           // The time at which Schedule was called.
}

// Set implements an in-order admission queue for actors requiring
// exclusive access to a set of keys.
//
// A Set is internally synchronized and is safe for concurrent use. A
// Set should not be copied after it has been created.
type Set[K comparable] struct {
	queue    *Queue[K, *waiter[K]] // Internally synchronized.
	runner   Runner                // Executes callbacks.
	waiterMu sync.Mutex            // Synchronizes access to waiters

	deferredStart    prometheus.Counter
	execCompleteTime prometheus.Observer
	execWaitTime     prometheus.Observer
	immediateStart   prometheus.Counter
	retriedTasks     prometheus.Counter
}

// New construct a Set that executes tasks using the given runner.
//
// See [GoRunner]
func New[K comparable](runner Runner, metricsLabel string) (*Set[K], error) {
	if runner == nil {
		return nil, errors.New("runner must not be nil")
	}
	if metricsLabel == "" {
		return nil, errors.New("metrics label must not be empty")
	}
	return &Set[K]{
		queue:  NewQueue[K, *waiter[K]](),
		runner: runner,

		deferredStart:    deferredStart.WithLabelValues(metricsLabel),
		execCompleteTime: execCompleteTime.WithLabelValues(metricsLabel),
		execWaitTime:     execWaitTime.WithLabelValues(metricsLabel),
		immediateStart:   immediateStart.WithLabelValues(metricsLabel),
		retriedTasks:     retriedTasks.WithLabelValues(metricsLabel),
	}, nil
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
	scheduleStart := time.Now()
	keys = dedup(keys)

	w := &waiter[K]{
		fn:            fn,
		keys:          keys,
		scheduleStart: scheduleStart,
	}
	w.result.Set(queued)
	ready, err := s.queue.Enqueue(keys, w)
	if err != nil {
		w.result.Set(StatusFor(err))
		return &w.result, func() {}
	}
	if ready {
		s.immediateStart.Inc()
		s.dispose(w, false)
	} else {
		s.deferredStart.Inc()
	}
	return &w.result, func() {
		// Swap the callback so that it does nothing. We want to guard
		// against revivifying an already completed waiter, so we
		// look at whether a function is still defined.
		s.waiterMu.Lock()
		needsDispose := w.fn != nil
		if needsDispose {
			w.fn = func([]K) error { return context.Canceled }
		}
		s.waiterMu.Unlock()

		// Async cleanup.
		if needsDispose {
			s.dispose(w, true)
		}
	}
}

// dispose of the waiter callback in a separate goroutine. The waiter
// will be dequeued from the Set, possibly leading to cascading
// callbacks.
func (s *Set[K]) dispose(w *waiter[K], cancel bool) {
	work := func(_ context.Context) {
		// Clear the function reference to make the effects of dispose a
		// one-shot.
		s.waiterMu.Lock()
		fn := w.fn
		w.fn = nil
		startedAtHead := s.queue.IsHead(w)
		s.waiterMu.Unlock()

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
			s.execWaitTime.Observe(time.Since(w.scheduleStart).Seconds())
			err = tryCall(fn, w.keys)
			s.execCompleteTime.Observe(time.Since(w.scheduleStart).Seconds())
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
				s.retriedTasks.Inc()

				// Otherwise, re-enable the waiter. The status will be
				// set to retryRequested for later re-dispatching by the
				// dispose method.
				s.waiterMu.Lock()
				w.fn = fn
				w.result.Set(retryRequested)
				endedAtHead := s.queue.IsHead(w)
				s.waiterMu.Unlock()

				// It's possible that another task completed while this
				// one was executing, which moved it to the head of the
				// global queue. If this happens, we need to immediately
				// queue up its retry.
				if !startedAtHead && endedAtHead {
					s.dispose(w, false)
				}

				// We can't dequeue the waiter if it's going to retry at
				// some later point in time. Since we know that the task
				// was running somewhere in the middle of the global
				// queue, there's nothing more that we need to do.
				return
			}
		default:
			w.result.Set(&Status{err: err})
		}

		// Remove the waiter's locks and get a slice of newly-unblocked
		// tasks to kick off.
		next, _ := s.queue.Dequeue(w)
		// Calling dequeue also advances the global queue. If the
		// element at the head of the queue wants to be retried, also
		// add it to the list.
		if head, ok := s.queue.PeekHead(); ok && head != nil {
			if status, _ := head.result.Get(); status == retryRequested {
				head.result.Set(retryQueued)
				next = append(next, head)
			}
		}
		for _, unblocked := range next {
			s.dispose(unblocked, false)
		}
	}

	if err := s.runner.Go(work); err != nil {
		w.result.Set(&Status{err: err})
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
