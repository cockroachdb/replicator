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
	"sync/atomic"
	"testing"

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

// Use random key sets and ensure that we don't see any collisions on
// the underlying resources.
func TestSmoke(t *testing.T) {
	const numResources = 128
	const numWaiters = 10 * numResources
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The checker function will toggle the values between 0 and a nonce
	// value to look for collisions.
	resources := make([]atomic.Int64, numResources)
	checker := func(keys []int) error {
		if len(keys) == 0 {
			return errors.New("no keys")
		}
		fail := false
		nonce := rand.Int63n(math.MaxInt64)
		for _, k := range keys {
			if !resources[k].CompareAndSwap(0, nonce) {
				fail = true
			}
		}
		for _, k := range keys {
			if !resources[k].CompareAndSwap(nonce, 0) {
				fail = true
			}
		}
		if fail {
			return errors.New("collision detected")
		}
		return nil
	}

	s := Set[int]{
		Runner: workgroup.WithSize(ctx, numWaiters/2, numResources),
	}
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
				keys[idx] = rand.Intn(numResources)
			}
			outcomes[i], _ = s.Schedule(keys, checker)
			return nil
		})
	}
	r.NoError(eg.Wait())

	// Wait for each task to arrive at a successful state.
	r.NoError(Wait(ctx, outcomes))
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
