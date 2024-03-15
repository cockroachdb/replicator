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

package lockset

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/workgroup"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Ensure serial ordering based on key.
func TestSerial(t *testing.T) {
	const numWaiters = 1024
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We want to verify that we see execution order for a key match the
	// scheduling order.
	var resource atomic.Int32
	checker := func(expect int) Callback[struct{}] {
		return func(keys []struct{}) error {
			current := resource.Add(1) - 1
			if expect != int(current) {
				return errors.New("out of order execution")
			}
			return nil
		}
	}

	var s Set[struct{}]
	outcomes := make([]*notify.Var[*Status], numWaiters)
	for i := 0; i < numWaiters; i++ {
		outcomes[i], _ = s.Schedule([]struct{}{{}}, checker(i))
	}

	r.NoError(Wait(ctx, outcomes))
}

// Use random key sets to ensure that we don't see any collisions on the
// underlying resources and that execution occurs in the expected order.
func TestSmoke(t *testing.T) {
	const numResources = 128
	const numWaiters = 10 * numResources
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Verify that each resource and waiter are run in the expected
	// order and the expeced number of times.
	executionCounts := make([]int, numWaiters)
	executionOrder := make([][]int, numResources)

	// The checker function will toggle the values between 0 and a nonce
	// value to look for collisions.
	resources := make([]atomic.Int64, numResources)
	checker := func(keys []int, retry bool, waiter int) error {
		if len(keys) == 0 {
			return errors.New("no keys")
		}
		executionCounts[waiter]++
		for _, k := range keys {
			executionOrder[k] = append(executionOrder[k], waiter)
		}
		fail := false
		nonce := rand.Int63n(math.MaxInt64)
		for _, k := range keys {
			if !resources[k].CompareAndSwap(0, nonce) {
				fail = true
			}
		}
		// Create goroutine scheduling jitter.
		runtime.Gosched()
		for _, k := range keys {
			if !resources[k].CompareAndSwap(nonce, 0) {
				fail = true
			}
		}
		if fail {
			return errors.New("collision detected")
		}
		if retry {
			return RetryAtHead(nil).Or(func() {
				// If the task was at the head of the global queue
				// already, this callback will be executed. We want to
				// add a fake execution entry to make comparison below
				// easy to think about.
				if executionCounts[waiter] != 2 {
					for _, k := range keys {
						executionOrder[k] = append(executionOrder[k], waiter)
					}
				}
			})
		}
		return nil
	}

	s := Set[int]{
		Runner: workgroup.WithSize(ctx, numWaiters/2, numResources),
	}

	expectedOrder := make([][]int, numResources)
	var expectedOrderMu sync.Mutex

	outcomes := make([]*notify.Var[*Status], numWaiters)
	eg, _ := errgroup.WithContext(ctx)
	for i := 0; i < numWaiters; i++ {
		i := i // Capture
		eg.Go(func() error {
			// Pick a random set of keys, intentionally including duplicate
			// key values.
			count := rand.Intn(numResources) + 1
			keys := make([]int, count)
			for idx := range keys {
				key := rand.Intn(numResources)
				keys[idx] = key
			}
			// We need to test against the same key deduplication that
			// the scheduler will perform when computing expected execution order.
			deduped := dedup(keys)
			willRetry := i%10 == 0
			expectedOrderMu.Lock()
			for _, key := range deduped {
				expectedOrder[key] = append(expectedOrder[key], i)
				if willRetry {
					expectedOrder[key] = append(expectedOrder[key], i)
				}
			}
			outcomes[i], _ = s.Schedule(keys, func(keys []int) error { return checker(keys, willRetry, i) })
			expectedOrderMu.Unlock()
			return nil
		})
	}
	r.NoError(eg.Wait())

	// Wait for each task to arrive at a successful state.
	r.NoError(Wait(ctx, outcomes))

	for i := 0; i < numResources; i++ {
		r.Equalf(expectedOrder[i], executionOrder[i], "key %d", i)
	}
}

func TestCancel(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var s Set[int]

	// Schedule a blocker first so we can control execution flow.
	blockCh := make(chan struct{})
	blocker, _ := s.Schedule([]int{0}, func([]int) error {
		<-blockCh
		return nil
	})

	// Schedule a job to cancel.
	canceled, cancel := s.Schedule([]int{0}, func([]int) error {
		return errors.New("should not see this")
	})
	status, _ := canceled.Get()
	r.True(status.Queued()) // This should always be true.
	cancel()                // The effects of cancel() are asynchronous.
	cancel()                // Duplicate cancel is a no-op.
	close(blockCh)          // Allow the machinery to proceed.

	// The blocker should be successful.
	r.NoError(Wait(ctx, []*notify.Var[*Status]{blocker}))

	for {
		status, changed := canceled.Get()
		// The cancel callback does set a trivial callback, so it's
		// possible that we could execute a callback which just returns
		// canceled.
		r.False(status.Success())
		if status.Err() != nil {
			r.ErrorIs(status.Err(), context.Canceled)
			break
		}
		<-changed
	}
}

func TestRunnerRejection(t *testing.T) {
	r := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := Set[int]{
		Runner: workgroup.WithSize(ctx, 1, 0),
	}

	block := make(chan struct{})

	// An empty key set will cause this to be executed immediately.
	s.Schedule(nil, func(keys []int) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	rejectedStatus, _ := s.Schedule(nil, func(keys []int) error {
		r.Fail("should not execute")
		return nil
	})
	rejected, _ := rejectedStatus.Get()
	r.ErrorContains(rejected.Err(), "queue depth 0 exceeded")
}

func TestPanic(t *testing.T) {
	r := require.New(t)

	var s Set[int]

	outcome, _ := s.Schedule(nil, func(keys []int) error {
		panic("boom")
	})

	for {
		status, changed := outcome.Get()
		if status.Err() != nil {
			r.ErrorContains(status.Err(), "boom")
			break
		}
		<-changed
	}

	outcome, _ = s.Schedule(nil, func(keys []int) error {
		panic(errors.New("boom"))
	})

	for {
		status, changed := outcome.Get()
		if status.Err() != nil {
			r.ErrorContains(status.Err(), "boom")
			break
		}
		<-changed
	}
}

func TestRetry(t *testing.T) {
	r := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var s Set[int]

	// This task will be at the head of the queue.
	block := make(chan struct{})
	blocker, _ := s.Schedule([]int{0}, func([]int) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// This task will retry itself and block the checker below.
	var didRetry, expectRetry atomic.Bool
	retried, _ := s.Schedule([]int{42}, func([]int) error {
		if expectRetry.CompareAndSwap(false, true) {
			// This error should never be seen.
			return RetryAtHead(errors.New("masked"))
		}

		if didRetry.CompareAndSwap(false, true) {
			// Retrying on a retry returns the error.
			return RetryAtHead(errors.New("should see this"))
		}

		r.Fail("called too many times")
		return nil
	})

	// Set up a task that depends upon the retried task. It shouldn't
	// execute until the retry has taken place.
	checker, _ := s.Schedule([]int{42}, func([]int) error {
		r.True(didRetry.Load())
		return nil
	})

	for {
		// Check that the blocker hasn't yet completed.
		status, _ := blocker.Get()
		r.False(status.Completed(),
			"expected uncompleted task, had %s", status)

		// Once we see the retry being requested, unblock the blocker.
		status, changed := retried.Get()
		if status.Retrying() {
			close(block)
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}

	// Wait for all tasks to complete.
	r.EqualError(
		Wait(ctx, []*notify.Var[*Status]{blocker, checker, retried}),
		"should see this")

	// Ensure that other tasks can still proceed.
	simple, _ := s.Schedule([]int{42}, func([]int) error {
		return nil
	})
	r.NoError(Wait(ctx, []*notify.Var[*Status]{simple}))
}

// This tests a case where a task requests rescheduling after another
// task promotes it to the head of the global queue.
func TestRetryAfterPromotion(t *testing.T) {
	r := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var s Set[int]

	blockPromoter := make(chan struct{})
	promoterOutcome, _ := s.Schedule(nil, func(keys []int) error {
		<-blockPromoter
		return nil
	})

	s.mu.Lock()
	promoterWaiter := s.mu.head
	s.mu.Unlock()
	r.NotNil(promoterWaiter)

	blockRetry := make(chan struct{})
	var retryCount atomic.Int32
	var retryRan atomic.Bool
	var retryWaiter *waiter[int]
	retryOutcome, _ := s.Schedule(nil, func([]int) error {
		if retryCount.Add(1) == 1 {
			<-blockRetry
		}
		return RetryAtHead(nil).Or(func() {
			// Ensure the tail was promoted.
			s.mu.Lock()
			r.Same(retryWaiter, s.mu.head)
			s.mu.Unlock()
			retryRan.Store(true)
		})
	})

	s.mu.Lock()
	retryWaiter = s.mu.tail
	s.mu.Unlock()

	r.NotNil(retryWaiter)
	r.NotSame(promoterWaiter, retryWaiter)

	close(blockPromoter)
	r.NoError(Wait(ctx, []Outcome{promoterOutcome}))
	close(blockRetry)

	r.NoError(Wait(ctx, []Outcome{retryOutcome}))
	r.True(retryRan.Load())
}

func TestStatusFor(t *testing.T) {
	r := require.New(t)

	r.True(StatusFor(nil).Success())
	r.False(StatusFor(context.Canceled).Success())
	r.ErrorIs(StatusFor(context.Canceled).Err(), context.Canceled)
}

func TestFakeOutcome(t *testing.T) {
	r := require.New(t)

	status, _ := NewOutcome().Get()
	r.True(status.Executing())
}

func TestDedup(t *testing.T) {
	r := require.New(t)

	src := []int{0, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 0}
	cpy := append([]int(nil), src...)
	expected := []int{0, 5, 4, 3, 2, 1}

	r.Equal(expected, dedup(src))
	// Ensure that the source was not modified.
	r.Equal(src, cpy)
}
