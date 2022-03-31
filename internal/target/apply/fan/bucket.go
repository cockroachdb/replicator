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
)

// A bucket manages mutations to be applied to a single table.
type bucket struct {
	applier types.Applier
	// This ensures that flushLoop() doesn't become completely wedged.
	applyTimeout time.Duration
	// A callback to notify that the bucket's consistent point has changed.
	onConsistent func(*bucket, stamp.Stamp)
	pool         pgxtype.Querier
	// stopped is closed when the bucket has shut down. This channel
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
	onConsistent func(*bucket, stamp.Stamp),
) *bucket {
	ret := &bucket{
		applier:      applier,
		applyTimeout: applyTimeout,
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
	return b.mu.work.Enqueue(newBatch(stamp, muts))
}

// Flush requests the bucket to flush any in-memory data it has. Results
// will be visible via the onConsistent callback.
func (b *bucket) Flush() {
	// Perform a non-blocking send to the wakeup channel. It has a size-1
	// buffer, so if there's already a pending flush, we don't need
	// to add another.
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

// Stop will cleanly halt the flush loop.
func (b *bucket) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.stopFlag {
		return
	}
	b.mu.stopFlag = true
	close(b.stopped)
}

// Stopped returns a channel which is closed once the bucket's flush
// goroutine has been stopped.
func (b *bucket) Stopped() <-chan struct{} {
	return b.stopped
}

// flushLoop is executed from a per-bucket goroutine.
func (b *bucket) flushLoop() {
	defer close(b.stopped)
	var consistent stamp.Stamp
outer:
	for {
		_, open := <-b.wakeup

		// Once woken up, drain everything that we have to send.
		for {
			ctx, cancel := context.WithTimeout(context.Background(), b.applyTimeout)
			more, err := b.flushOneBatch(ctx)
			cancel()
			if err != nil {
				log.WithError(err).Warn("error while flushing mutations; will retry")
				continue outer
			}
			if !more {
				break
			}
		}

		// If the consistent point advanced, send a callback.
		if fn := b.onConsistent; fn != nil {
			nextConsistent := b.Consistent()
			if stamp.Compare(nextConsistent, consistent) > 0 {
				fn(b, nextConsistent)
			}
		}

		if !open {
			log.Trace("bucket.flushLoop exiting")
			return
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

	if err := b.applier.Apply(ctx, b.pool, toApply.Pick()); err != nil {
		return false, err
	}

	// Now, we can drain the element that we peeked at above.
	b.mu.Lock()
	drained := b.mu.work.Dequeue()
	b.mu.Unlock()

	// Sanity-check that we drained the correct element.
	if toApply != drained.(*stampedBatch) {
		return false, errors.New("did not drain expected element")
	}
	return true, nil
}
