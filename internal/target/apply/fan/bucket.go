// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fan

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// A bucket manages mutations to be applied to a single table.
type bucket struct {
	applier types.Applier
	// This ensures that flushLoop() doesn't become completely wedged.
	applyTimeout time.Duration
	// This semaphore is owned by the Fan.
	backpressure *semaphore.Weighted
	// A callback to notify that the bucket's consistent point has changed.
	onConsistent func(*bucket, stamp.Stamp)
	pool         pgxtype.Querier
	// stopped is closed when flushLoop() method returns. This channel
	// is provided to external callers, so it is independent of the
	// internal wakeup channel below.
	stopped chan struct{}
	// Send a message to kick the flush loop; close to stop it.
	wakeup chan struct{}

	mu struct {
		sync.Mutex
		stopFlag bool // Set when the stopped channel is closed.
		work     stamp.Queue
	}
}

// newBucket starts a per-bucket goroutine. Callers must call the Stop
// method to terminate it.
func newBucket(
	applier types.Applier,
	applyTimeout time.Duration,
	pool pgxtype.Querier,
	backpressure *semaphore.Weighted,
	onConsistent func(*bucket, stamp.Stamp),
) *bucket {
	ret := &bucket{
		applier:      applier,
		applyTimeout: applyTimeout,
		backpressure: backpressure,
		pool:         pool,
		onConsistent: onConsistent,
		stopped:      make(chan struct{}),
		wakeup:       make(chan struct{}, 1),
	}

	go ret.flushLoop()

	return ret
}

// Consistent returns the consistent point for the bucket.
func (b *bucket) Consistent() stamp.Stamp {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.work.Consistent()
}

// Enqueue adds mutations to be processed and their associated stamp.
func (b *bucket) Enqueue(stamp stamp.Stamp, muts []types.Mutation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.stopFlag {
		return errors.New("the bucket has been stopped")
	}

	// Apply backpressure based on the number of bytes in the mutations.
	toAcquire := int64(0)
	for _, mut := range muts {
		toAcquire += mutationWeight(mut)
	}
	// This should succeed in the majority case.  If not, the target
	// cluster is likely under-sized or the connection pool is too
	// small.
	if !b.backpressure.TryAcquire(toAcquire) {
		log.Debugf("applying backpressure for %d bytes", toAcquire)
		backpressureEncountered.Inc()
		ctx, cancel := context.WithTimeout(context.Background(), b.applyTimeout)
		defer cancel()
		if err := b.backpressure.Acquire(ctx, toAcquire); err != nil {
			return errors.Wrapf(err, "could not acquire %d bytes of backpressure", toAcquire)
		}
		log.Debugf("backpressure relieved")
	}

	return b.mu.work.Enqueue(newBatch(stamp, muts))
}

// Flush requests the bucket to flush any in-memory data it has. Results
// will be visible via the onConsistent callback.
func (b *bucket) Flush() {
	// Perform a non-blocking send to the wakeup channel. It has a small
	// buffer, so if there's already a pending flush, we don't need to
	// add another.
	select {
	case b.wakeup <- struct{}{}:
	default:
	}
}

// Mark indicates that the stamp may become a consistent point in the
// future.
func (b *bucket) Mark(stamp stamp.Stamp) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.work.Mark(stamp)
}

// Stop will cleanly halt the flush loop. Callers should wait for
// the Stopped channel to close.
func (b *bucket) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.stopFlag {
		return
	}
	b.mu.stopFlag = true

	// Enqueue a final flush.
	b.Flush()
	// Close the wakeup channel, which will make flushLoop() exit.
	log.Trace("bucket closing wakeup channel")
	close(b.wakeup)
}

// Stopped returns a channel which is closed once the bucket's flush
// goroutine has been stopped.
func (b *bucket) Stopped() <-chan struct{} {
	return b.stopped
}

// flushLoop is executed from a per-bucket goroutine.
func (b *bucket) flushLoop() {
	defer close(b.stopped)
	consistent := b.Consistent()
	callbackFn := b.onConsistent
outer:
	for range b.wakeup {
		// Once woken up, drain everything that we have to send, until
		// we run out of work to perform.
		for {
			ctx, cancel := context.WithTimeout(context.Background(), b.applyTimeout)
			more, err := b.flushOneBatch(ctx)
			cancel()
			if err != nil {
				log.WithError(err).Warn("error while flushing mutations; will retry")
				continue outer
			}

			// If the consistent point advanced, send a callback.
			if callbackFn != nil {
				nextConsistent := b.Consistent()
				if stamp.Compare(nextConsistent, consistent) > 0 {
					callbackFn(b, nextConsistent)
					consistent = nextConsistent
				}
			}
			if !more {
				break
			}
		}
	}
}

func (b *bucket) flushOneBatch(ctx context.Context) (more bool, _ error) {
	// Peek at the queued value. We don't want to dequeue it here if the
	// mutations cannot be applied due to a transient error.
	b.mu.Lock()
	peeked := b.mu.work.Peek()
	b.mu.Unlock()

	if peeked == nil {
		return false, nil
	}

	toApply := peeked.(*stampedBatch)

	muts := toApply.Pick()
	if err := b.applier.Apply(ctx, b.pool, muts); err != nil {
		return false, err
	}

	// Now, we can drain the element that we peeked at above.
	b.mu.Lock()
	drained := b.mu.work.Dequeue()
	more = b.mu.work.Peek() != nil
	b.mu.Unlock()

	// Release backpressure bytes in shared semaphore.
	toRelease := int64(0)
	for _, mut := range muts {
		toRelease += mutationWeight(mut)
	}
	b.backpressure.Release(toRelease)

	// Sanity-check that we drained the correct element.
	if toApply != drained.(*stampedBatch) {
		return false, errors.New("did not drain expected element")
	}
	return more, nil
}

// mutationWeight computes a weight to use for generating backpressure.
func mutationWeight(mut types.Mutation) int64 {
	return int64(len(mut.Data)) + int64(len(mut.Key))
}
