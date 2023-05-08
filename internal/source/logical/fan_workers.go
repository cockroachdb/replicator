// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"
	"sync"

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
	pool         types.Querier
	stamp        stamp.Stamp
	workerStatus func() error

	mu struct {
		sync.Mutex
		data  map[ident.Table][]types.Mutation
		drain bool       // Graceful drain condition.
		wake  *sync.Cond // Locks on mu.
	}
}

// newFanWorkers constructs and starts a pool of workers to apply
// mutations to destination tables.
func newFanWorkers(ctx context.Context, loop *loop, stamp stamp.Stamp) *fanWorkers {
	ret := &fanWorkers{
		appliers:  loop.factory.appliers,
		batchSize: batches.Size(),
		pool:      loop.targetPool,
		stamp:     stamp,
	}
	ret.mu.data = make(map[ident.Table][]types.Mutation)
	ret.mu.wake = sync.NewCond(&ret.mu.Mutex)

	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < loop.config.FanShards; i++ {
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
	t.mu.data[table] = append(t.mu.data[table], mut...)
	// Wake one worker to act on the new data.
	t.mu.wake.Signal()
	return nil
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
	// Wake all workers to respond to the stopping signal.
	t.mu.wake.Broadcast()
	t.mu.Unlock()

	// sync.Cond doesn't offer an interruptable wait, so fake one.
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

// loop waits for mutations to be enqueued and then applies them.
func (t *fanWorkers) loop(ctx context.Context) error {
	for {
		table, muts, moreWork := t.waitForWork()
		if !moreWork {
			return nil
		}
		applier, err := t.appliers.Get(ctx, table)
		if err != nil {
			return errors.Wrapf(err, "table %s", table)
		}
		if err := applier.Apply(ctx, t.pool, muts); err != nil {
			return errors.Wrapf(err, "table %s", table)
		}
	}
}

// waitForWork finds some mutations to apply to a table. If there are no
// remaining mutations and the fanWorkers is stopped, this method will
// return false.
func (t *fanWorkers) waitForWork() (table ident.Table, mut []types.Mutation, moreWork bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		// Find a non-empty slice of mutations.
		for table, mut = range t.mu.data {
			if count := len(mut); count > 0 {
				// Limit number of values dequeued.
				if count > t.batchSize {
					t.mu.data[table] = mut[t.batchSize:]
					mut = mut[:t.batchSize]
				} else {
					// Consuming all values, so delete the entry.
					delete(t.mu.data, table)
				}
				moreWork = true
				return
			}
		}
		// Shutdown flag set by Wait.
		if t.mu.drain {
			return
		}
		t.mu.wake.Wait()
	}
}
