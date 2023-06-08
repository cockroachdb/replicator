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

// Package stopper contains a utility class for gracefully terminating
// long-running processes.
package stopper

import (
	"context"
	"errors"
	"sync"
	"time"
)

// contextKey is a [context.Context.Value] key.
type contextKey struct{}

// background is a Context that never stops.
var background = &Context{
	delegate: context.Background(),
	stopping: make(chan struct{}),
}

// ErrStopped will be returned from [context.Cause] when the Context has
// been stopped.
var ErrStopped = errors.New("stopped")

// ErrGracePeriodExpired will be returned from [context.Cause] when the
// Context has been stopped, but the goroutines have not exited.
var ErrGracePeriodExpired = errors.New("grace period expired")

// A Context is conceptually similar to an [errgroup.Group] in that it
// manages a [context.Context] whose lifecycle is associated with some
// number of goroutines. Rather than canceling the associated context
// when a goroutine returns an error, it cancels the context after the
// Stop method is called and all associated goroutines have all exited.
//
// As an API convenience, the Context type implements [context.Context]
// so that it fits into idiomatic context-plumbing.  The [From]
// function can be used to retrieve a Context from any
// [context.Context].
type Context struct {
	cancel   func(error)
	delegate context.Context
	stopping chan struct{}
	parent   *Context

	mu struct {
		sync.RWMutex
		count    int
		err      error
		stopping bool
	}
}

var _ context.Context = (*Context)(nil)

// Background is analogous to [context.Background]. It returns a Context
// which cannot be stopped or canceled, but which is otherwise
// functional.
func Background() *Context { return background }

// From returns a pre-existing Context from the Context chain. Use
// [WithContext] to construct a new Context.
//
// If the chain is not associated with a Context, the [Background]
// instance will be returned.
func From(ctx context.Context) *Context {
	if s, ok := ctx.(*Context); ok {
		return s
	}
	if s := ctx.Value(contextKey{}); s != nil {
		return s.(*Context)
	}
	return Background()
}

// IsStopping is a convenience method to determine if a stopper is
// associated with a Context and if work should be stopped.
func IsStopping(ctx context.Context) bool {
	return From(ctx).IsStopping()
}

// WithContext creates a new Context whose work will be immediately
// canceled when the parent context is canceled. If the provided context
// is already managed by a Context, a call to the enclosing
// [Context.Stop] method will also trigger a call to Stop in the
// newly-constructed Context.
func WithContext(ctx context.Context) *Context {
	// Might be background, which never stops.
	parent := From(ctx)

	ctx, cancel := context.WithCancelCause(ctx)
	s := &Context{
		cancel:   cancel,
		delegate: ctx,
		parent:   parent,
		stopping: make(chan struct{}),
	}

	// Propagate a parent stop or context cancellation into a Stop call
	// to ensure that all notification channels are closed.
	go func() {
		select {
		case <-parent.Stopping():
		case <-s.Done():
		}
		s.Stop(0)
	}()
	return s
}

// Deadline implements [context.Context].
func (s *Context) Deadline() (deadline time.Time, ok bool) { return s.delegate.Deadline() }

// Done implements [context.Context]. The channel that is returned will
// be closed when Stop has been called and all associated goroutines
// have exited. The returned channel will be closed immediately if the
// parent context (passed to [WithContext]) is canceled.
func (s *Context) Done() <-chan struct{} { return s.delegate.Done() }

// Err implements context.Context. When the return value for this is
// [context.ErrCanceled], [context.Cause] will return [ErrStopped] if
// the context cancellation resulted from a call to Stop.
func (s *Context) Err() error { return s.delegate.Err() }

// Go spawns a new goroutine to execute the given function and monitors
// its lifecycle. This method will not execute the function and return
// false if Stop has already been called. If the function returns an
// error, the Stop method will be called. The returned error will be
// available from Wait once the remaining goroutines have exited.
func (s *Context) Go(fn func() error) (accepted bool) {
	if !s.apply(1) {
		return false
	}

	go func() {
		defer s.apply(-1)
		if err := fn(); err != nil {
			s.Stop(0)
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.mu.err == nil {
				s.mu.err = err
			}
		}
	}()
	return true
}

// IsStopping returns true once [Stop] has been called.  See also
// [Stopping] for a notification-based API.
func (s *Context) IsStopping() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.stopping
}

// Stop begins a graceful shutdown of the Context. When this method is
// called, the Stopping channel will be closed.  Once all goroutines
// started by Go have exited, the associated Context will be cancelled,
// thus closing the Done channel. If the gracePeriod is non-zero, the
// context will be forcefully cancelled if the goroutines have not
// exited within the given timeframe.
func (s *Context) Stop(gracePeriod time.Duration) {
	if s == background {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.stopping {
		return
	}
	s.mu.stopping = true
	close(s.stopping)

	// Cancel the context if nothing's currently running.
	if s.mu.count == 0 {
		s.cancel(ErrStopped)
	} else if gracePeriod > 0 {
		go func() {
			select {
			case <-time.After(gracePeriod):
				// Cancel after the grace period has expired. This
				// should immediately terminate any well-behaved
				// goroutines driven by Go().
				s.cancel(ErrGracePeriodExpired)
			case <-s.Done():
				// We'll hit this path in a clean-exit, where apply()
				// cancels the context after the last goroutine has
				// exited.
			}
		}()
	}
}

// Stopping returns a channel that is closed when a graceful shutdown
// has been requested or when the parent context has been canceled.
func (s *Context) Stopping() <-chan struct{} {
	return s.stopping
}

// Value implements context.Context.
func (s *Context) Value(key any) any {
	if _, ok := key.(contextKey); ok {
		return s
	}
	return s.delegate.Value(key)
}

// Wait will block until Stop has been called and all associated
// goroutines have exited or the parent context has been cancelled. This
// method will return the first, non-nil error from any of the callbacks
// passed to Go. If Wait is called on the [Background] instance, it will
// immediately return nil.
func (s *Context) Wait() error {
	if s == background {
		return nil
	}
	<-s.Done()

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.err
}

// apply is used to maintain the count of started goroutines. It returns
// true if the delta was applied.
func (s *Context) apply(delta int) bool {
	if s == background {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Don't allow new goroutines to be added when stopping.
	if s.mu.stopping && delta >= 0 {
		return false
	}

	// Ensure that nested jobs prolong the lifetime of the parent
	// context to prevent premature cancellation. Verify that the parent
	// accepted the delta in case it was just stopped, but our helper
	// goroutine hasn't yet called Stop on this instance.
	if !s.parent.apply(delta) {
		return false
	}

	s.mu.count += delta
	if s.mu.count < 0 {
		// Implementation error, not user problem.
		panic("over-released")
	}
	if s.mu.count == 0 && s.mu.stopping {
		s.cancel(ErrStopped)
	}
	return true
}
