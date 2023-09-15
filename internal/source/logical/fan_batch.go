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
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// fanBatch is an implementation of the Batch interface that applies
// mutations using a fixed pool of goroutines.
type fanBatch struct {
	batchSize    int
	pending      *latch.Counter
	parent       *loop
	workerStatus func() error // Blocking call to get errgroup status.

	mu struct {
		sync.RWMutex
		data         *ident.TableMap[[]types.Mutation] // Mutations to apply.
		drain        bool                              // Graceful drain condition.
		updated      chan struct{}                     // Closed and replaced when mu changes.
		workerExited bool                              // Set when any loop() worker has returned.
	}
}

var _ Batch = (*fanBatch)(nil)

// newFanBatch constructs and starts a pool of workers to apply
// mutations to destination tables.
func newFanBatch(ctx context.Context, loop *loop) *fanBatch {
	ret := &fanBatch{
		batchSize: batches.Size(),
		parent:    loop,
		pending:   latch.New(),
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

// Flush returns nil when all in-flight mutations have been applied.
// This is useful for ensuring that dependency ordering of mutations is
// maintained.
func (b *fanBatch) Flush(ctx context.Context) error {
	// Check that worker goroutines are still running.
	checkErr := func() (waitFor <-chan struct{}, err error) {
		b.mu.RLock()
		muUpdated := b.mu.updated
		exited := b.mu.workerExited
		// Not deferred, since b.workerStatus() is blocking.
		b.mu.RUnlock()

		if exited {
			return nil, errors.Wrap(b.workerStatus(), "error during concurrent loop behavior")
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
	workDone := b.pending.Wait()

	for {
		select {
		case <-workDone:
			// No work in flight, ideal state.
			return nil

		case <-checkForError:
			// State in fanBatch has changed, re-verify state.
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

// OnCommit implements Events.
func (b *fanBatch) OnCommit(_ context.Context) <-chan error {
	return b.notifyOnCompletion(true /* drain */)
}

// OnData implements Batch. This method may be called before Start,
// however it may not be called again once a terminal method has been
// called.
func (b *fanBatch) OnData(
	_ context.Context, _ ident.Ident, table ident.Table, mut []types.Mutation,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.drain {
		return errors.New("OnData() after OnCommit() / OnRollback()")
	}
	b.pending.Apply(len(mut))
	b.mu.data.Put(table, append(b.mu.data.GetZero(table), mut...))
	close(b.mu.updated)
	b.mu.updated = make(chan struct{})
	return nil
}

// OnRollback implements Events and resets any pending work.
func (b *fanBatch) OnRollback(ctx context.Context) error {
	select {
	case err := <-b.notifyOnCompletion(false /* abandon */):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// chaos sometimes returns an error for testing.
func (b *fanBatch) chaos() error {
	if prob := b.parent.factory.baseConfig.ChaosProb; prob != 0 && rand.Float32() < prob {
		return errors.New("fanBatch.chaos")
	}
	return nil
}

// loop waits for mutations to be enqueued and then applies them.
func (b *fanBatch) loop(ctx context.Context) error {
	appliers := b.parent.factory.appliers
	targetPool := b.parent.factory.targetPool

	// Loop extracted to be able to use defer keyword for cleanup.
	tryLoop := func() (bool, error) {
		table, muts, moreWork := b.waitForWork(ctx)
		if !moreWork {
			return false, nil
		}
		// Ensure that callers to Flush() aren'b blocked if mutations
		// can'b be applied.
		defer b.pending.Apply(-len(muts))

		// Simulate error when trying to apply data.
		if err := b.chaos(); err != nil {
			return false, err
		}
		applier, err := appliers.Get(ctx, table)
		if err != nil {
			return false, errors.Wrapf(err, "table %s", table)
		}
		if err := applier.Apply(ctx, targetPool, muts); err != nil {
			return false, errors.Wrapf(err, "table %s", table)
		}
		return true, nil
	}

	// Set a flag to indicate that one or more loop goroutines have
	// exited.
	defer func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if !b.mu.workerExited {
			b.mu.workerExited = true
			close(b.mu.updated)
			b.mu.updated = make(chan struct{})
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

// notifyOnCompletion prevents new mutations from being enqueued and
// returns once all workers have become idle. The drain flag, if set,
// allows all enqueued mutations to be processed before returning.
func (b *fanBatch) notifyOnCompletion(gracefulDrain bool) <-chan error {
	b.mu.Lock()
	b.mu.drain = true
	// Dump all mutations if we don't want a graceful drain.
	if !gracefulDrain {
		b.mu.data = &ident.TableMap[[]types.Mutation]{}
	}
	close(b.mu.updated)
	b.mu.updated = make(chan struct{})
	b.mu.Unlock()

	// Retrieve the worker status asynchronously.
	ch := make(chan error, 1)
	go func() {
		ch <- b.workerStatus()
		close(ch)
	}()
	return ch
}

// Used by waitForWork to break out of a Range loop.
var errStop = errors.New("ignored")

// waitForWork finds some mutations to apply to a table. If there are no
// remaining mutations and the fanBatch is stopped, this method will
// return false. The wait can be interrupted by cancelling the context.
func (b *fanBatch) waitForWork(
	ctx context.Context,
) (table ident.Table, mut []types.Mutation, moreWork bool) {
	tryDequeue := func() (waitFor chan struct{}) {
		b.mu.Lock()
		defer b.mu.Unlock()

		// Find a non-empty slice of mutations.
		_ = b.mu.data.Range(func(candidateTbl ident.Table, candidateMut []types.Mutation) error {
			// Copy variables into outer scope.
			table = candidateTbl
			mut = candidateMut

			if count := len(mut); count > 0 {
				// Limit number of values dequeued.
				if count > b.batchSize {
					b.mu.data.Put(table, mut[b.batchSize:])
					mut = mut[:b.batchSize]
					// Ensure another worker is woken to consume the
					// remainder of the slice.
					close(b.mu.updated)
					b.mu.updated = make(chan struct{})
				} else {
					// Consuming all values.
					b.mu.data.Delete(table)
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
		if b.mu.drain {
			return nil
		}
		return b.mu.updated
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
