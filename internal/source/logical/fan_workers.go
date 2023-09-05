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

package logical

import (
	"context"
	"math/rand"
	"sync"

	"github.com/bobvawter/latch"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// fanWorkers accumulates mutations to apply to one or more tables by
// way of a fixed pool of goroutines.
type fanWorkers struct {
	appliers     types.Appliers
	batchSize    int
	chaosProb    float32 // Set in tests via BaseConfig.ChaosProb.
	pending      *latch.Counter
	targetPool   *types.TargetPool
	stamp        stamp.Stamp
	workerStatus func() error // Blocking call to get errgroup status.

	mu struct {
		sync.RWMutex
		data         *ident.TableMap[[]types.Mutation] // Mutations to apply.
		drain        bool                              // Graceful drain condition.
		updated      chan struct{}                     // Closed and replaced when mu changes.
		workerExited bool                              // Set when any loop() worker has returned.
	}
}

// newFanWorkers constructs and starts a pool of workers to apply
// mutations to destination tables.
func newFanWorkers(ctx context.Context, loop *loop, stamp stamp.Stamp) *fanWorkers {
	ret := &fanWorkers{
		appliers:   loop.factory.appliers,
		batchSize:  batches.Size(),
		chaosProb:  loop.factory.baseConfig.ChaosProb,
		pending:    latch.New(),
		targetPool: loop.factory.targetPool,
		stamp:      stamp,
	}
	ret.mu.data = &ident.TableMap[[]types.Mutation]{}
	ret.mu.updated = make(chan struct{})

	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < loop.factory.baseConfig.FanShards; i++ {
		eg.Go(func() error {
			return ret.loop(egCtx)
		})
	}
	ret.workerStatus = eg.Wait

	return ret
}

// Enqueue adds mutations to be applied to the given table. This method
// may be called before Start, however it may not be called again once
// Wait has been called.
func (t *fanWorkers) Enqueue(table ident.Table, mut []types.Mutation) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.drain {
		return errors.New("Enqueue() after Wait()")
	}
	t.pending.Apply(len(mut))
	t.mu.data.Put(table, append(t.mu.data.GetZero(table), mut...))
	close(t.mu.updated)
	t.mu.updated = make(chan struct{})
	return nil
}

// Flush returns nil when all in-flight mutations have been applied.
// This is useful for ensuring that dependency ordering of mutations is
// maintained.
func (t *fanWorkers) Flush(ctx context.Context) error {
	// Check that worker goroutines are still running.
	checkErr := func() (waitFor <-chan struct{}, err error) {
		t.mu.RLock()
		muUpdated := t.mu.updated
		exited := t.mu.workerExited
		// Not deferred, since t.workerStatus() is blocking.
		t.mu.RUnlock()

		if exited {
			return nil, errors.Wrap(t.workerStatus(), "error during concurrent loop behavior")
		}
		return muUpdated, nil
	}

	// Perform an early check for loops that have already exited.
	checkForError, err := checkErr()
	if err != nil {
		return err
	}

	// Request notification when the count of in-flight mutations
	// reaches zero (perhaps instantaneously).
	workDone := t.pending.Wait()

	for {
		select {
		case <-workDone:
			// No work in flight, ideal state.
			return nil

		case <-checkForError:
			// State in fanWorkers has changed, re-verify state.
			checkForError, err = checkErr()
			if err != nil {
				return err
			}

		case <-ctx.Done():
			// Cancelled.
			return ctx.Err()
		}
	}
}

// Stamp implement stamp.Stamped.
func (t *fanWorkers) Stamp() stamp.Stamp {
	return t.stamp
}

// Wait prevents new mutations from being enqueued and returns once all
// workers have become idle. The drain flag, if set, allows all enqueued
// mutations to be processed before returning.
func (t *fanWorkers) Wait(ctx context.Context, drain bool) error {
	t.mu.Lock()
	t.mu.drain = true
	// Dump all mutations if we don't want a graceful drain.
	if !drain {
		t.mu.data = nil
	}
	close(t.mu.updated)
	t.mu.updated = make(chan struct{})
	t.mu.Unlock()

	// Retrieve the worker status asynchronously.
	ch := make(chan error, 1)
	go func() {
		ch <- t.workerStatus()
	}()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// chaos sometimes returns an error for testing.
func (t *fanWorkers) chaos() error {
	if t.chaosProb != 0 && rand.Float32() < t.chaosProb {
		return errors.New("fanWorkers.chaos")
	}
	return nil
}

// loop waits for mutations to be enqueued and then applies them.
func (t *fanWorkers) loop(ctx context.Context) error {
	// Loop extracted to be able to use defer keyword for cleanup.
	tryLoop := func() (bool, error) {
		table, muts, moreWork := t.waitForWork(ctx)
		if !moreWork {
			return false, nil
		}
		// Ensure that callers to Flush() aren't blocked if mutations
		// can't be applied.
		defer t.pending.Apply(-len(muts))

		// Simulate error when trying to apply data.
		if err := t.chaos(); err != nil {
			return false, err
		}
		applier, err := t.appliers.Get(ctx, table)
		if err != nil {
			return false, errors.Wrapf(err, "table %s", table)
		}
		if err := applier.Apply(ctx, t.targetPool, muts); err != nil {
			return false, errors.Wrapf(err, "table %s", table)
		}
		return true, nil
	}

	// Set a flag to indicate that one or more loop goroutines have
	// exited.
	defer func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.mu.workerExited {
			t.mu.workerExited = true
			close(t.mu.updated)
			t.mu.updated = make(chan struct{})
		}
	}()

	for {
		running, err := tryLoop()
		if err != nil {
			// This error will be available from workerStatus.
			return err
		}
		if !running {
			return nil
		}
	}
}

// Used by waitForWork to break out of a Range loop.
var errStop = errors.New("ignored")

// waitForWork finds some mutations to apply to a table. If there are no
// remaining mutations and the fanWorkers is stopped, this method will
// return false. The wait can be interrupted by cancelling the context.
func (t *fanWorkers) waitForWork(
	ctx context.Context,
) (table ident.Table, mut []types.Mutation, moreWork bool) {
	tryDequeue := func() (waitFor chan struct{}) {
		t.mu.Lock()
		defer t.mu.Unlock()

		// Find a non-empty slice of mutations.
		_ = t.mu.data.Range(func(candidateTbl ident.Table, candidateMut []types.Mutation) error {
			// Copy variables into outer scope.
			table = candidateTbl
			mut = candidateMut

			if count := len(mut); count > 0 {
				// Limit number of values dequeued.
				if count > t.batchSize {
					t.mu.data.Put(table, mut[t.batchSize:])
					mut = mut[:t.batchSize]
					// Ensure another worker is woken to consume the
					// remainder of the slice.
					close(t.mu.updated)
					t.mu.updated = make(chan struct{})
				} else {
					// Consuming all values.
					t.mu.data.Delete(table)
				}
				moreWork = true
				// We found work, so return a sentinel error.
				return errStop
			}
			return nil
		})
		// Found work to do, no need to wait.
		if moreWork {
			return nil
		}
		// Shutdown flag set by Wait.
		if t.mu.drain {
			return nil
		}
		return t.mu.updated
	}

	for {
		waitFor := tryDequeue()
		if waitFor == nil {
			return
		}
		select {
		case <-waitFor:
			// Something changed, loop around.
		case <-ctx.Done():
			return
		}
	}
}
