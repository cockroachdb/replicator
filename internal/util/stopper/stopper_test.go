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

package stopper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAmbient(t *testing.T) {
	a := assert.New(t)

	s := From(context.Background())
	a.Same(s, background)

	a.False(IsStopping(context.Background()))
	s.Go(func() error { return nil })

	// Should be a no-op.
	s.Stop(0)
	a.False(s.mu.stopping)
	a.Nil(s.Err())
	a.Nil(s.Wait())
}

func TestCancelOuter(t *testing.T) {
	a := assert.New(t)

	top, cancelTop := context.WithCancel(context.Background())

	s := WithContext(top)

	s.Go(func() error { <-s.Done(); return nil })

	cancelTop()
	select {
	case <-s.Stopping():
	// Verify that canceling the top-level also closes the Stopping channel.
	case <-time.After(time.Second):
		a.Fail("timed out waiting for Stopping to close")
	}
	a.True(IsStopping(s))
	a.ErrorIs(s.Err(), context.Canceled)
	a.ErrorIs(context.Cause(s), context.Canceled)
	a.Nil(s.Wait())
}

func TestCallbackErrorStops(t *testing.T) {
	a := assert.New(t)

	s := WithContext(context.Background())
	err := errors.New("BOOM")
	s.Go(func() error { return err })
	a.ErrorIs(s.Wait(), err)
	a.Error(context.Cause(s), ErrStopped)
}

func TestChainStopper(t *testing.T) {
	a := assert.New(t)

	parent := WithContext(context.Background())
	mid := context.WithValue(parent, parent, parent) // Demonstrate unwrapping.
	child := WithContext(mid)
	a.Same(parent, child.parent)

	waitFor := make(chan struct{})
	child.Go(func() error { <-waitFor; return nil })

	// Verify that stopping the parent propagates to the child.
	parent.Stop(0)
	select {
	case <-child.Stopping():
	// OK
	case <-time.After(time.Second):
		a.Fail("call to stop did not propagate")
	}

	// However, the contexts should not cancel until the work is done.
	a.Nil(parent.Err())
	a.Nil(child.Err())

	// Allow the work to finish, and verify cancellation.
	close(waitFor)

	select {
	case <-child.Done():
	// OK
	case <-time.After(time.Second):
		a.Fail("timeout waiting for child to finish")
	}

	select {
	case <-mid.Done():
	// OK
	case <-time.After(time.Second):
		a.Fail("timeout waiting for mid to finish")
	}

	select {
	case <-parent.Done():
	// OK
	case <-time.After(time.Second):
		a.Fail("timeout waiting for parent to finish")
	}

	a.ErrorIs(child.Err(), context.Canceled)
	a.ErrorIs(context.Cause(child), ErrStopped)
	a.Nil(child.Wait())

	a.ErrorIs(mid.Err(), context.Canceled)
	a.ErrorIs(context.Cause(mid), ErrStopped)

	a.ErrorIs(parent.Err(), context.Canceled)
	a.ErrorIs(context.Cause(parent), ErrStopped)
	a.Nil(child.Wait())
}

func TestGracePeriod(t *testing.T) {
	a := assert.New(t)

	s := WithContext(context.Background())

	// This goroutine waits on Done, which is not correct.
	s.Go(func() error { <-s.Done(); return nil })

	s.Stop(time.Nanosecond)

	<-s.Done()
	a.ErrorIs(s.Err(), context.Canceled)
	a.ErrorIs(context.Cause(s), ErrGracePeriodExpired)
}

func TestStopper(t *testing.T) {
	a := assert.New(t)

	s := WithContext(context.Background())
	a.Same(s, From(s))                          // Direct cast
	a.Same(s, From(context.WithValue(s, s, s))) // Unwrapping
	select {
	case <-s.Stopping():
		a.Fail("should not be stopping yet")
	default:
		// OK
	}
	a.False(IsStopping(s))

	waitFor := make(chan struct{})
	a.True(s.Go(func() error { <-waitFor; return nil }))
	a.True(s.Go(func() error { return nil }))

	s.Stop(0)
	select {
	case <-s.Stopping():
	// OK
	case <-time.After(time.Second):
		a.Fail("timeout waiting for stopped")
	}

	// Verify that the context is stopping, but not cancelled.
	a.True(IsStopping(s))
	a.Nil(s.Err())

	// It's a no-op to run new routines after stopping.
	a.False(s.Go(func() error { return nil }))

	// Stop the waiting goroutines.
	close(waitFor)

	// Once all workers have stopped, the context should cancel.
	select {
	case <-s.Done():
	// OK
	case <-time.After(time.Second):
		a.Fail("timeout waiting for Context.Done()")
	}
	a.True(IsStopping(s))
	a.NotNil(s.Err())
	a.ErrorIs(context.Cause(s), ErrStopped)
	a.Nil(s.Wait())
}

// Verify that a never-used Stopper behaves correctly.
func TestUnused(t *testing.T) {
	a := assert.New(t)

	s := WithContext(context.Background())
	s.Stop(0)
	select {
	case <-s.Done():
	// OK
	case <-time.After(time.Second):
		a.Fail("timeout waiting for Context.Done()")
	}
	a.ErrorIs(context.Cause(s), ErrStopped)
	a.Nil(s.Wait())
}
