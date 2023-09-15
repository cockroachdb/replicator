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
	"github.com/bobvawter/latch"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"math/rand"
)

// fanBatch is an implementation of the Batch interface that applies
// mutations using a fixed pool of goroutines.
type fanBatch struct {
	batchSize    int
	pending      *latch.Counter
	parent       *loop
	state        notify.Var[*fanBatchState]
	workerStatus func() error // Blocking call to get errgroup status.
}

var _ Batch = (*fanBatch)(nil)

// Internal state for a single fanBatch. Should only be mutated while
// holding [fanBatch.state] lock.
type fanBatchState struct {
	data         *ident.TableMap[[]types.Mutation] // Mutations to apply.
	drain        bool                              // Graceful drain condition.
	workerExited bool                              // Set when any loop() worker has returned.
}

// newFanBatch constructs and starts a pool of workers to apply
// mutations to destination tables.
func newFanBatch(ctx context.Context, loop *loop) *fanBatch {
	ret := &fanBatch{
		batchSize: batches.Size(),
		parent:    loop,
		pending:   latch.New(),
	}
	ret.state.Set(&fanBatchState{
		data: &ident.TableMap[[]types.Mutation]{},
	})

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
		var exited bool
		updated, _ := b.state.Peek(func(state *fanBatchState) error {
			exited = state.workerExited
			return nil
		})

		if exited {
			return nil, errors.Wrap(b.workerStatus(), "error during concurrent loop behavior")
		}
		return updated, nil
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
	_, _, err := b.state.Update(func(state *fanBatchState) (*fanBatchState, error) {
		if state.drain {
			return nil, errors.New("OnData() after OnCommit() / OnRollback()")
		}
		state.data.Put(table, append(state.data.GetZero(table), mut...))
		b.pending.Apply(len(mut))
		return state, nil
	})
	return err
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
		table, muts := b.waitForWork(ctx)
		if len(muts) == 0 {
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
	defer b.state.Update(func(state *fanBatchState) (*fanBatchState, error) {
		state.workerExited = true
		return state, nil
	})

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
	_, _, _ = b.state.Update(func(state *fanBatchState) (*fanBatchState, error) {
		state.drain = true
		// Dump all mutations if we don't want a graceful drain.
		if !gracefulDrain {
			state.data = &ident.TableMap[[]types.Mutation]{}
		}
		return state, nil
	})

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
) (table ident.Table, mut []types.Mutation) {
	for {
		shouldReturn := false

		// We mutate the state here, so call Update instead of Peek.
		_, waitFor, _ := b.state.Update(func(state *fanBatchState) (*fanBatchState, error) {
			// Find a non-empty slice of mutations.
			_ = state.data.Range(func(candidateTbl ident.Table, candidateMut []types.Mutation) error {
				// Copy variables into outer scope.
				table = candidateTbl
				mut = candidateMut
				count := len(mut)

				if count == 0 {
					return nil // continue Range
				}
				shouldReturn = true

				// Limit number of values dequeued.
				if count > b.batchSize {
					state.data.Put(table, mut[b.batchSize:])
					mut = mut[:b.batchSize]
				} else {
					// Consume all values.
					state.data.Delete(table)
				}
				return errStop // break from Range
			})

			// Shutdown flag set by notifyOnCompletion. We'll either
			// return the data that was found or return no data, leading
			// the caller to exit.
			if state.drain {
				shouldReturn = true
			}
			return state, nil
		})

		if shouldReturn {
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
