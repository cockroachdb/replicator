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
	"golang.org/x/sync/semaphore"
)

// A ConsistentCallback is passed to Fans.New.
type ConsistentCallback func(stamp.Stamp) error

// Fans is a factory for constructing Fan instances.
type Fans struct {
	Appliers types.Appliers
	Pool     pgxtype.Querier
}

// New returns a new Fan-out helper with the given configuration.
func (f *Fans) New(
	applyTimeout time.Duration, onConsistent ConsistentCallback, shardCount int, bytesInFlight int,
) (_ *Fan, cancel func(), _ error) {
	if onConsistent == nil {
		onConsistent = func(stamp.Stamp) error { return nil }
	}

	ret := &Fan{
		appliers:          f.Appliers,
		applyTimeout:      applyTimeout,
		backpressureBytes: bytesInFlight,
		onConsistent:      onConsistent,
		pool:              f.Pool,
		shardCount:        shardCount,
		stopped:           make(chan struct{}),
		wakeup:            make(chan struct{}, 1),
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

// Fan allows a serial stream of mutations to be applied across
// concurrent SQL connections to the target cluster. Instances of Fan
// are constructed via Fans.
type Fan struct {
	appliers          types.Appliers
	applyTimeout      time.Duration
	backpressureBytes int
	onConsistent      ConsistentCallback
	pool              pgxtype.Querier
	shardCount        int           // Number of per-table buckets.
	stopped           chan struct{} // Externally visible.
	wakeup            chan struct{} // Kicks the flush loop when state changes.

	// The data in this struct is generational, based on calls to Reset.
	mu struct {
		sync.Mutex

		backpressure *semaphore.Weighted
		buckets      map[bucketKey]*bucket
		minMap       *stamp.MinMap // Keys are *bucket.
		stopFlag     bool          // Set when canceled.
	}
}

// Enqueue mutations with the given stamp.
//
// The value provided to stamp must be monotonic.
func (f *Fan) Enqueue(
	ctx context.Context, stamp stamp.Stamp, table ident.Table, muts []types.Mutation,
) error {
	f.mu.Lock()
	stopped := f.mu.stopFlag
	f.mu.Unlock()

	if stopped {
		return errors.New("already stopped")
	}

	for _, mut := range muts {
		bucket, err := f.bucketFor(ctx, table, mut)
		if err != nil {
			return err
		}
		// The bucket will coalesce tail values.
		if err := bucket.Enqueue(stamp, mut); err != nil {
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
		return f.onConsistent(s)
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

	// Stop all buckets. Nil-check because we use Reset() from New()
	if f.mu.buckets != nil {
		log.Tracef("stopping %d buckets", len(f.mu.buckets))
		for _, bucket := range f.mu.buckets {
			bucket.Stop()
		}
		log.Tracef("stopped %d buckets", len(f.mu.buckets))
	}

	f.mu.backpressure = semaphore.NewWeighted(int64(f.backpressureBytes))
	f.mu.buckets = make(map[bucketKey]*bucket)
	f.mu.minMap = stamp.NewMinMap()
}

// Stopped returns a channel that is closed when the Fan has been
// shut down.
func (f *Fan) Stopped() <-chan struct{} {
	return f.stopped
}

// bucketForLocked finds or creates a new shard bucket for applying
// mutations to the given table.
func (f *Fan) bucketFor(
	ctx context.Context, table ident.Table, mut types.Mutation,
) (*bucket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := keyFor(table, f.shardCount, mut)
	if found, ok := f.mu.buckets[key]; ok {
		return found, nil
	}

	app, err := f.appliers.Get(ctx, table)
	if err != nil {
		return nil, err
	}
	ret := newBucket(app, f.applyTimeout, f.pool, f.mu.backpressure, f.onBucketConsistent)
	f.mu.buckets[key] = ret

	// Insert the bucket into the minmap here, so that we don't get
	// spurious changed notifications later on.
	f.mu.minMap.Put(ret, nil)

	return ret, nil
}

// flush adds an entry to the channel that controls the behavior of
// flushLoop. If a flush is already pending, this method does nothing.
func (f *Fan) flush() {
	select {
	case f.wakeup <- struct{}{}:
	default:
	}
}

// flushLoop is executed from a dedicated goroutine.
func (f *Fan) flushLoop() {
	// Propagate requests to flush each bucket.
	for range f.wakeup {
		f.mu.Lock()
		for _, bucket := range f.mu.buckets {
			// This call to Flush is non-blocking.
			bucket.Flush()
		}
		f.mu.Unlock()
	}

	// Tell each bucket to shut down.
	f.mu.Lock()
	for _, bucket := range f.mu.buckets {
		// This call to Stop is non-blocking.
		bucket.Stop()
	}
	f.mu.Unlock()

	log.Debug("fan instance quiesced")
}

// Handle notifications from each shard's bucket. When all shards have
// agreed on a new consistent value, we can report that back to the
// owner.
func (f *Fan) onBucketConsistent(b *bucket, consistent stamp.Stamp) error {
	var min stamp.Stamp
	var changed bool

	f.mu.Lock()
	// Verify that the bucket is part of the map. This ensures that a
	// bucket that pre-dates a call to Reset won't be able to modify any
	// currently-visible state.
	if _, ok := f.mu.minMap.Get(b); ok {
		min, changed = f.mu.minMap.Put(b, consistent)
	}
	f.mu.Unlock()

	// Perform callback outside critical section.
	if changed {
		return f.onConsistent(min)
	}
	return nil
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
	defer close(f.stopped)

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
	close(f.wakeup)

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
