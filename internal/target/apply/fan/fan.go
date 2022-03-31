// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package fan provides a parallel-commit process for applying mutations
// across a sharded row-space.
package fan

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A ConsistentCallback is passed to New
type ConsistentCallback func(stamp.Stamp)

// Fan allows a serial stream of mutations to be applied across
// concurrent SQL connections to the target cluster.
type Fan struct {
	appliers     types.Appliers
	applyTimeout time.Duration
	onConsistent ConsistentCallback
	pool         pgxtype.Querier
	shardCount   int           // Number of per-table buckets.
	stopped      chan struct{} // Closed to stop the flush loop.
	wakeup       chan struct{} // Kicks the flush loop when state changes.

	mu struct {
		sync.RWMutex

		buckets  map[bucketKey]*bucket
		minMap   *stamp.MinMap // Keys are *bucket.
		stopFlag bool          // Set when canceled.
	}
}

// New constructs a Fan instance and returns a cancellation
// function which will perform a graceful shutdown.
func New(
	a types.Appliers,
	applyTimeout time.Duration,
	pool pgxtype.Querier,
	onConsistent ConsistentCallback,
	shardCount int,
) (_ *Fan, cancel func(), _ error) {
	ret := &Fan{
		appliers:     a,
		applyTimeout: applyTimeout,
		onConsistent: onConsistent,
		pool:         pool,
		shardCount:   shardCount,
		stopped:      make(chan struct{}),
		wakeup:       make(chan struct{}, 2),
	}
	if ret.onConsistent == nil {
		ret.onConsistent = func(s stamp.Stamp) {}
	}
	// Initialize other data members.
	ret.Reset()

	go ret.flushLoop()

	return ret, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := ret.stop(ctx); err != nil {
			log.WithError(err).Warn("fan helper did not shut down cleanly")
		}
	}, nil
}

// Enqueue mutations with the given stamp.
//
// The value provided to stamp must be monotonic.
func (f *Fan) Enqueue(
	ctx context.Context, stamp stamp.Stamp, table ident.Table, muts []types.Mutation,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.stopFlag {
		return errors.New("already stopped")
	}

	for _, mut := range muts {
		bucket, err := f.bucketForLocked(ctx, table, mut)
		if err != nil {
			return err
		}
		// The bucket will coalesce tail values.
		if err := bucket.Enqueue(stamp, muts); err != nil {
			return err
		}
	}
	f.flush()
	return nil
}

// Mark is given a stamp which is guaranteed to be strictly less than
// any future stamp provided to Enqueue. Marked stamps are used to
// provide consistent points for transactional consistency.
func (f *Fan) Mark(s stamp.Stamp) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// If there are no active buckets, we treat the mark as being
	// consistent.
	if len(f.mu.buckets) == 0 {
		f.onConsistent(s)
		return nil
	}

	for _, bucket := range f.mu.buckets {
		if err := bucket.Mark(s); err != nil {
			return err
		}
	}
	f.flush()
	return nil
}

// Reset abandons all in-memory mutations.
func (f *Fan) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.buckets = make(map[bucketKey]*bucket)
	f.mu.minMap = stamp.NewMinMap()
}

// bucketForLocked finds or creates a new shard bucket for applying
// mutations to the given table.
func (f *Fan) bucketForLocked(
	ctx context.Context, table ident.Table, mut types.Mutation,
) (*bucket, error) {
	key := keyFor(table, f.shardCount, mut)
	if found, ok := f.mu.buckets[key]; ok {
		return found, nil
	}

	// TODO(bob): Make this configurable?
	app, err := f.appliers.Get(ctx, table, nil /* cas */, types.Deadlines{})
	if err != nil {
		return nil, err
	}
	ret := newBucket(app, f.applyTimeout, f.pool, f.onBucketConsistent)
	f.mu.buckets[key] = ret

	// Insert the bucket into the minmap here, so that we don't get
	// spurious changed notifications later on.
	f.mu.minMap.Put(ret, nil)

	return ret, nil
}

// flush adds an entry to the length-1 channel that controls the
// behavior of flushLoop. If a flush is already pending, this method
// does nothing.
func (f *Fan) flush() {
	select {
	case f.wakeup <- struct{}{}:
	default:
	}
}

// flushLoop is executed from a dedicated goroutine.
func (f *Fan) flushLoop() {
	var stopped bool
	for {
		select {
		case <-f.stopped:
			log.Trace("fan flushLoop exiting")
			stopped = true
		case <-f.wakeup:
			// case <-time.After(time.Second):
		}
		f.mu.Lock()
		for _, bucket := range f.mu.buckets {
			bucket.Flush()
			if stopped {
				bucket.Stop()
			}
		}
		f.mu.Unlock()
		if stopped {
			log.Debug("fan instance quiesced")
			return
		}
	}
}

// Handle notifications from each shard's bucket. When all shards have
// agreed on a new consistent value, we can report that back to the
// owner.
func (f *Fan) onBucketConsistent(b *bucket, stamp stamp.Stamp) {
	f.mu.Lock()
	min, changed := f.mu.minMap.Put(b, stamp)
	f.mu.Unlock()

	if changed {
		f.onConsistent(min)
	}
}

// stop will shut down the flush loop. This method blocks until all
// managed buckets have finished stopping. The context here is used
// to provide a timeout for shutting down.
func (f *Fan) stop(ctx context.Context) error {
	f.mu.Lock()
	if f.mu.stopFlag {
		f.mu.Unlock()
		return nil
	}
	f.mu.stopFlag = true

	// Snapshot the buckets here, so we can unlock the mutex, allowing
	// the flushLoop below to also read the map.
	waitFor := make([]*bucket, len(f.mu.buckets))
	idx := 0
	for _, bucket := range f.mu.buckets {
		waitFor[idx] = bucket
		idx++
	}
	f.mu.Unlock()

	// This will trigger a wakeup in flushLoop, which will propagate
	// the call to bucket.stop.
	close(f.stopped)

	// Wait for all buckets to clean up.
	for _, bucket := range waitFor {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-bucket.Stopped():
		}
	}
	return nil
}
