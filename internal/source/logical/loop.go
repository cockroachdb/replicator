// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package logical provides a common logical-replication loop behavior.
package logical

import (
	"context"
	"encoding"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Loop provides a common feature set for processing single-stream,
// logical replication feeds. This can be used to build feeds from any
// data source which has well-defined "consistent points", such as
// write-ahead-log offsets or explicit transaction ids.
type Loop struct {
	loop         *loop
	initialPoint stamp.Stamp
}

// AwaitConsistentPoint waits until the consistent point has advanced to
// the requested value or until the context is cancelled.
func (l *Loop) AwaitConsistentPoint(
	ctx context.Context, comparison AwaitComparison, point stamp.Stamp,
) (stamp.Stamp, error) {
	return l.loop.AwaitConsistentPoint(ctx, comparison, point)
}

// Dialect returns the logical.Dialect in use.
func (l *Loop) Dialect() Dialect {
	return l.loop.dialect
}

// GetConsistentPoint returns current consistent-point stamp.
func (l *Loop) GetConsistentPoint() stamp.Stamp {
	return l.loop.GetConsistentPoint()
}

// GetInitialPoint returns the consistent-point stamp that the
// replication loop started at. This can be compared to
// GetConsistentPoint to determine the amount of progress that has been
// made.
func (l *Loop) GetInitialPoint() stamp.Stamp {
	return l.initialPoint
}

// Stopped returns a channel that is closed when the Loop has shut down.
func (l *Loop) Stopped() <-chan struct{} {
	return l.loop.stopped
}

// loop is not internally synchronized; it assumes that it is being
// driven by a serial stream of data.
type loop struct {
	// The active configuration.
	config *BaseConfig
	// The Dialect contains message-processing, specific to a particular
	// source database.
	dialect Dialect
	// Various strategies for implementing the Events interface.
	events struct {
		fan    Events
		serial Events
	}
	// The Factory that created the loop.
	factory *Factory
	// Optional checkpoint saved into the target database
	memo    types.Memo
	stopped chan struct{}
	// Used to update the consistentPoint in the target database.
	targetPool pgxtype.Querier

	// This represents a position in the source's transaction log.
	// The value in this struct should only be accessed when holding
	// the condition lock.
	consistentPoint struct {
		*sync.Cond
		stamp stamp.Stamp
	}

	metrics struct {
		backfillStatus prometheus.Gauge
	}
}

// loadConsistentPoint will return the latest consistent-point stamp,
// the value of Config.DefaultConsistentPoint, or Dialect.ZeroStamp.
func (l *loop) loadConsistentPoint(ctx context.Context) (stamp.Stamp, error) {
	ret := l.dialect.ZeroStamp()
	data, err := l.memo.Get(ctx, l.targetPool, l.config.LoopName)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 {
		return ret, json.Unmarshal(data, ret)
	}
	// Support bootstrapping the consistent point from flag values.
	if l.config.DefaultConsistentPoint != "" {
		if x, ok := ret.(encoding.TextUnmarshaler); ok {
			return ret, x.UnmarshalText([]byte(l.config.DefaultConsistentPoint))
		}
	}
	return ret, nil
}

// setConsistentPoint is safe to call from any goroutine. It will
// occasionally persist the consistent point to the memo table.
func (l *loop) setConsistentPoint(p stamp.Stamp) error {
	l.consistentPoint.L.Lock()
	defer l.consistentPoint.L.Unlock()

	// Notify Dialect instances that have explicit coordination needs
	// that the consistent point is about to advance.
	if cb, ok := l.dialect.(ConsistentCallback); ok {
		if err := cb.OnConsistent(p); err != nil {
			return errors.Wrap(err, "consistent point not advancing")
		}
	}

	log.Tracef("loop %s new consistent point %s -> %s", l.config.LoopName, l.consistentPoint.stamp, p)
	l.consistentPoint.stamp = p
	l.consistentPoint.Broadcast()

	if err := l.storeConsistentPoint(p); err != nil {
		return errors.Wrap(err, "could not persistent consistent point")
	}
	log.Tracef("Saved checkpoint for %s", l.config.LoopName)
	return nil
}

// storeConsistentPoint commits the given stamp to the memo table.
func (l *loop) storeConsistentPoint(p stamp.Stamp) error {
	data, err := json.Marshal(p)
	if err != nil {
		return errors.WithStack(err)
	}
	return l.memo.Put(context.Background(),
		l.targetPool, l.config.LoopName, data,
	)
}

// AwaitConsistentPoint implements State.
func (l *loop) AwaitConsistentPoint(
	ctx context.Context, comparison AwaitComparison, point stamp.Stamp,
) (stamp.Stamp, error) {
	// Fast-path
	if found := l.GetConsistentPoint(); stamp.Compare(found, point) >= int(comparison) {
		return found, nil
	}

	// Use a separate goroutine to allow cancellation.
	result := make(chan stamp.Stamp, 1)
	go func() {
		defer close(result)
		l.consistentPoint.L.Lock()
		defer l.consistentPoint.L.Unlock()
		for ctx.Err() == nil {
			found := l.consistentPoint.stamp
			if stamp.Compare(found, point) >= int(comparison) {
				result <- found
				return
			}
			l.consistentPoint.Wait()
		}
	}()

	select {
	case found := <-result:
		// We know that the goroutine above must have exited.
		return found, nil

	case <-ctx.Done():
		// Ensure that the goroutine above can exit.
		l.consistentPoint.L.Lock()
		l.consistentPoint.Broadcast()
		l.consistentPoint.L.Unlock()

		return nil, ctx.Err()
	}
}

// GetConsistentPoint implements State.
func (l *loop) GetConsistentPoint() stamp.Stamp {
	l.consistentPoint.L.Lock()
	defer l.consistentPoint.L.Unlock()
	return l.consistentPoint.stamp
}

// GetTargetDB implements State.
func (l *loop) GetTargetDB() ident.Ident {
	return l.config.TargetDB
}

// run blocks while the connection is processing messages.
func (l *loop) run(ctx context.Context) {
	defer log.Debugf("replication loop %q shut down", l.config.LoopName)
	defer close(l.stopped)

	for {
		err := l.runOnce(ctx)

		// If the outer context is done, just return.
		if ctx.Err() != nil {
			log.Tracef("loop %s outer context done", l.config.LoopName)
			return
		}

		// Otherwise, log any error, and sleep for a bit.
		if err != nil {
			log.WithError(err).Errorf("error in replication loop %s; retrying in %s",
				l.config.LoopName, l.config.RetryDelay)
		}

		// On a clean exit, we still want to sleep for a bit.
		select {
		case <-time.After(l.config.RetryDelay):
		case <-ctx.Done():
			return
		}
	}
}

// runOnce is called by run.
func (l *loop) runOnce(ctx context.Context) error {
	if lessor, ok := l.dialect.(Lessor); ok {
		// Loop until we can acquire a lease.
		var lease types.Lease
		for {
			var err error
			lease, err = lessor.Acquire(ctx)
			// Lease acquired.
			if err == nil {
				log.Tracef("lease %s acquired", l.config.LoopName)
				break
			}
			// If busy, wait until the expiration.
			if busy, ok := types.IsLeaseBusy(err); ok {
				log.WithField("until", busy.Expiration).Tracef(
					"lease %s was busy, waiting", l.config.LoopName)

				// Add some jitter to the expiration.
				duration := time.Until(busy.Expiration) +
					time.Duration(rand.Intn(10))*time.Millisecond

				select {
				case <-time.After(duration):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			// General err, defer to the loop's retry delay.
			return err
		}
		defer lease.Release()
		// Ensure that all work is bound to the lifetime of the lease.
		ctx = lease.Context()
	}

	// Ensure our in-memory consistent point matches the database.
	point, err := l.loadConsistentPoint(ctx)
	if err != nil {
		return err
	}
	l.consistentPoint.L.Lock()
	l.consistentPoint.stamp = point
	l.consistentPoint.L.Unlock()

	// Determine how to perform the filling.
	source, events, isBackfilling := l.chooseFillStrategy()
	// Ensure that we're in a clear state when recovering.
	defer events.stop()

	if err := l.runOnceUsing(ctx, source, events, isBackfilling); err != nil {
		return err
	}

	// Ensure that the latest point has been saved on a clean exit.
	return errors.Wrapf(l.storeConsistentPoint(l.GetConsistentPoint()),
		"could not save consistent point for %s", l.config.LoopName)
}

// runOnceUsing is called from runOnce or doBackfill.
func (l *loop) runOnceUsing(
	ctx context.Context, source fillFn, events Events, isBackfilling bool,
) error {
	if isBackfilling {
		l.metrics.backfillStatus.Set(1)
	} else {
		l.metrics.backfillStatus.Set(0)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	// Start a background goroutine to maintain the replication
	// connection. This source goroutine is set up to be robust; if
	// there's an error talking to the source database, we send a
	// rollback message to the consumer and retry the connection.
	ch := make(chan Message, 16)
	group.Go(func() error {
		defer close(ch)
		for ctx.Err() == nil {
			err := source(ctx, ch, events)

			// Return if the source closed cleanly (we may switch
			// from backfill to streaming modes) or if the outer
			// context is being shut down.
			if err == nil || ctx.Err() != nil {
				return nil
			}

			// Otherwise, we'll recover by injecting a new rollback
			// message and then restarting the message stream from
			// the previous consistent point.
			log.WithError(err).Errorf(
				"error from replication source %s; continuing",
				l.config.LoopName)
			select {
			case ch <- msgRollback:
				continue
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	// This goroutine applies the incoming mutations to the target
	// database. It is fragile, when it errors, we need to also
	// restart the source goroutine.
	group.Go(func() error {
		err := l.dialect.Process(ctx, ch, events)

		// As above.
		if err == nil || ctx.Err() != nil {
			return nil
		}

		log.WithError(err).Errorf(
			"error while applying replication messages %s; stopping",
			l.config.LoopName)
		return err
	})

	// Toggle backfilling mode as necessary by canceling the work
	// context. This will restart the loop, choosing the appropriate
	// replication mode.
	_, canBackfill := l.dialect.(Backfiller)
	canBackfill = canBackfill && l.config.BackfillWindow > 0
	if isBackfilling || canBackfill {
		group.Go(func() error {
			defer cancel()

			point := l.dialect.ZeroStamp()
			for {
				// Wait for the consistent point to advance. This method
				// only returns an error if the context has been
				// canceled.
				point, _ = l.AwaitConsistentPoint(ctx, AwaitGT, point)
				if ctx.Err() != nil {
					return nil
				}
				ts, ok := point.(TimeStamp)
				if !ok {
					log.Warn("dialect implements Backfiller, but doesn't use TimeStamp stamps")
					return nil
				}
				delta := time.Since(ts.AsTime())
				if isBackfilling {
					if delta < l.config.BackfillWindow {
						log.WithFields(log.Fields{
							"loop": l.config.LoopName,
							"ts":   ts,
						}).Debug("backfill has caught up")
						return nil
					}
				} else if canBackfill {
					if delta > l.config.BackfillWindow {
						log.WithFields(log.Fields{
							"loop": l.config.LoopName,
							"ts":   ts,
						}).Warn("replication has fallen behind, switching to backfill mode")
						return nil
					}
				}
			}
		})
	}

	return group.Wait()
}

type fillFn = func(context.Context, chan<- Message, State) error

// chooseFillStrategy returns the strategy that will be used for
// generating replication messages.
func (l *loop) chooseFillStrategy() (choice fillFn, events Events, isBackfill bool) {
	choice = l.dialect.ReadInto
	if l.config.Immediate {
		events = l.events.fan
	} else {
		events = l.events.serial
	}
	// Is backfilling supported?
	back, ok := l.dialect.(Backfiller)
	if !ok {
		return
	}
	// Is backfilling enabled by the user?
	if l.config.BackfillWindow <= 0 {
		return
	}
	// Is the last consistent point sufficiently old to backfill?
	ts, ok := l.GetConsistentPoint().(TimeStamp)
	if !ok {
		return
	}
	delta := time.Since(ts.AsTime())
	if delta < l.config.BackfillWindow {
		return
	}
	choice = back.BackfillInto
	events = l.events.fan
	isBackfill = true
	log.WithFields(log.Fields{
		"delta": delta,
		"loop":  l.config.LoopName,
		"ts":    ts,
	}).Debug("using backfill strategy")
	return
}

// doBackfill provides the implementation of Events.Backfill.
func (l *loop) doBackfill(
	ctx context.Context, loopName string, backfiller Backfiller, options ...Option,
) error {
	options = append(options, WithName(loopName))
	cfg := l.config.Copy()
	for _, option := range options {
		option(cfg)
	}

	filler, err := l.factory.newLoop(ctx, cfg, backfiller)
	if err != nil {
		return err
	}

	return filler.loop.runOnceUsing(ctx, backfiller.BackfillInto, filler.loop.events.fan, true)
}
