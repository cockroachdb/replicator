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

// Package logical provides a common logical-replication loop behavior.
package logical

import (
	"context"
	"encoding"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// Loop provides a common feature set for processing single-stream,
// logical replication feeds. This can be used to build feeds from any
// data source which has well-defined "consistent points", such as
// write-ahead-log offsets or explicit transaction ids.
type Loop struct {
	loop         *loop
	initialPoint stamp.Stamp
}

// Dialect returns the logical.Dialect in use.
func (l *Loop) Dialect() Dialect {
	return l.loop.loopConfig.Dialect
}

// GetConsistentPoint returns current consistent-point stamp and a
// channel that will be closed when the value has changed again.
func (l *Loop) GetConsistentPoint() (stamp.Stamp, <-chan struct{}) {
	return l.loop.consistentPoint.Get()
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
	return l.loop.running.Done()
}

// loop is not internally synchronized; it assumes that it is being
// driven by a serial stream of data.
type loop struct {
	// Various strategies for implementing the Events interface.
	events struct {
		fan    Events
		serial Events
	}
	// The Factory that created the loop.
	factory *Factory
	// Loop-specific configuration.
	loopConfig *LoopConfig
	// This is controlled by the call to run. That is, when run exits,
	// this Context will be stopped.
	running *stopper.Context

	// This represents a position in the source's transaction log.
	consistentPoint notify.Var[stamp.Stamp]

	metrics struct {
		backfillStatus prometheus.Gauge
	}
}

var (
	_ diag.Diagnostic = (*loop)(nil)
	_ State           = (*loop)(nil)
)

// loadConsistentPoint will return the latest consistent-point stamp,
// the value of Config.DefaultConsistentPoint, or Dialect.ZeroStamp.
func (l *loop) loadConsistentPoint(ctx context.Context) (stamp.Stamp, error) {
	ret := l.loopConfig.Dialect.ZeroStamp()
	data, err := l.factory.memo.Get(ctx, l.factory.stagingPool, l.loopConfig.LoopName)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 {
		return ret, json.Unmarshal(data, ret)
	}
	// Support bootstrapping the consistent point from flag values.
	if l.loopConfig.DefaultConsistentPoint != "" {
		if x, ok := ret.(encoding.TextUnmarshaler); ok {
			return ret, x.UnmarshalText([]byte(l.loopConfig.DefaultConsistentPoint))
		}
	}
	return ret, nil
}

// Diagnostic implements [diag.Diagnostic].
func (l *loop) Diagnostic(ctx context.Context) any {
	ret := make(map[string]any)

	ret["config"] = l.loopConfig
	if x, ok := l.loopConfig.Dialect.(diag.Diagnostic); ok {
		ret["dialect"] = x.Diagnostic(ctx)
	}
	ret["stamp"], _ = l.GetConsistentPoint()

	return ret
}

// GetConsistentPoint implements State.
func (l *loop) GetConsistentPoint() (stamp.Stamp, <-chan struct{}) {
	return l.consistentPoint.Get()
}

// GetTargetDB implements State.
func (l *loop) GetTargetDB() ident.Schema {
	return l.loopConfig.TargetSchema
}

// SetConsistentPoint implements State and is safe to call from any
// goroutine. It will persist the consistent point to the memo table.
func (l *loop) SetConsistentPoint(_ context.Context, next stamp.Stamp) error {
	_, _, err := l.consistentPoint.Update(func(old stamp.Stamp) (stamp.Stamp, error) {
		if c := stamp.Compare(next, old); c < 0 {
			return nil, errors.Errorf("consistent point going backwards: %s vs %s",
				next, old)
		} else if c == 0 {
			return nil, errors.Errorf("consistent point stalled: %s", next)
		}
		log.Tracef("loop %s new consistent point %s -> %s",
			l.loopConfig.LoopName, old, next)
		return next, nil
	})
	if err != nil {
		return err
	}

	if err := l.storeConsistentPoint(next); err != nil {
		return errors.Wrap(err, "could not persistent consistent point")
	}
	log.Tracef("Saved checkpoint for %s", l.loopConfig.LoopName)
	return nil
}

// Stopping implements State. See also [stopperEvents], which is passed
// to the Dialect implementations with a per-iteration stop channel.
func (l *loop) Stopping() <-chan struct{} {
	return l.running.Stopping()
}

// run blocks while the connection is processing messages.
func (l *loop) run() {
	defer log.Debugf("replication loop %q shut down", l.loopConfig.LoopName)

	for {
		err := l.runOnce()

		// Otherwise, log any error, and sleep for a bit.
		if err != nil {
			log.WithError(err).Errorf("error in replication loop %s; retrying in %s",
				l.loopConfig.LoopName, l.factory.baseConfig.RetryDelay)
		}

		select {
		case <-time.After(l.factory.baseConfig.RetryDelay):
			// On a clean exit, we still want to sleep for a bit before
			// running a new iteration.
			continue
		case <-l.running.Stopping():
			// If a clean shutdown is requested, just exit.
			return
		}
	}
}

// runOnce is called by run. If the Dialect implements a leasing
// behavior, a lease will be obtained before any further action is
// taken.
func (l *loop) runOnce() error {
	var stop *stopper.Context
	if lessor, ok := l.loopConfig.Dialect.(Lessor); ok {
		// Loop until we can acquire a lease.
		var lease types.Lease
		for {
			var err error
			lease, err = lessor.Acquire(l.running)
			// Lease acquired.
			if err == nil {
				log.Tracef("lease %s acquired", l.loopConfig.LoopName)
				break
			}
			// If busy, wait until the expiration.
			if busy, ok := types.IsLeaseBusy(err); ok {
				log.WithField("until", busy.Expiration).Tracef(
					"lease %s was busy, waiting", l.loopConfig.LoopName)

				// Add some jitter to the expiration.
				duration := time.Until(busy.Expiration) +
					time.Duration(rand.Intn(10))*time.Millisecond

				select {
				case <-time.After(duration):
					continue
				case <-l.running.Stopping():
					return nil
				}
			}
			// General err, defer to the loop's retry delay.
			return err
		}
		defer lease.Release()
		// Ensure that all work is bound to the lifetime of the lease.
		stop = stopper.WithContext(lease.Context())
	} else {
		stop = l.running
	}

	// Ensure our in-memory consistent point matches the database.
	point, err := l.loadConsistentPoint(stop)
	if err != nil {
		return err
	}
	l.consistentPoint.Set(point)

	// Determine how to perform the filling.
	source, events, isBackfilling := l.chooseFillStrategy()

	return l.runOnceUsing(stop, source, events, isBackfilling)
}

// runOnceUsing is called from runOnce or doBackfill.
func (l *loop) runOnceUsing(
	ctx *stopper.Context, source fillFn, events Events, isBackfilling bool,
) error {
	if isBackfilling {
		l.metrics.backfillStatus.Set(1)
	} else {
		l.metrics.backfillStatus.Set(0)
	}

	// Calls to the parent Stop() are automatically chained.
	ctx = stopper.WithContext(ctx)

	// Make the stopping channel available to the dialect.
	events = &stopperEvents{events, ctx.Stopping()}

	// Start a background goroutine to maintain the replication
	// connection. This source goroutine is set up to be robust; if
	// there's an error talking to the source database, we send a
	// rollback message to the consumer and retry the connection.
	ch := make(chan Message, 16)
	ctx.Go(func() error {
		defer close(ch)

		for {
			err := source(ctx, ch, events)

			// Return if the source closed cleanly. This will close the
			// communication channel, allowing the processor goroutine
			// to exit gracefully.
			if err == nil {
				return nil
			}

			select {
			case <-ctx.Stopping():
				return nil
			case ch <- msgRollback:
				// We'll recover by injecting a new rollback message,
				// waiting for a bit, and then restarting the message
				// stream from the previous consistent point.
				log.WithError(err).Errorf(
					"error from replication source %s; continuing",
					l.loopConfig.LoopName)
			case <-ctx.Done():
				return ctx.Err()
			default:
				return errors.New("stopping loop due to consumer backpressure during rollback")
			}

			// Interruptable sleep since RetryDelay is likely several
			// seconds in a reasonable configuration.
			select {
			case <-time.After(l.factory.baseConfig.RetryDelay):
			case <-ctx.Stopping():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// This goroutine applies the incoming mutations to the target
	// database.
	ctx.Go(func() error {
		err := l.loopConfig.Dialect.Process(ctx, ch, events)
		// It's an error for Process to return unless the channel
		// has been closed (i.e. we're switching modes).
		if err == nil && !ctx.IsStopping() {
			err = errors.Errorf("%T.Process() returned unexpectedly", l.loopConfig.Dialect)
		}
		return errors.Wrap(err, "error while applying replication messages")
	})

	// Toggle backfilling mode as necessary by triggering a drain.
	// This will restart the loop, choosing the appropriate
	// replication mode.
	_, canBackfill := l.loopConfig.Dialect.(Backfiller)
	canBackfill = canBackfill && l.factory.baseConfig.BackfillWindow > 0
	if isBackfilling || canBackfill {
		ctx.Go(func() error {
			point, pointChanged := l.consistentPoint.Get()
			for {
				ts, ok := point.(TimeStamp)
				if !ok {
					log.Warn("dialect implements Backfiller, but doesn't use TimeStamp stamps")
					return nil
				}
				delta := time.Since(ts.AsTime())
				if isBackfilling {
					if delta < l.factory.baseConfig.BackfillWindow {
						log.WithFields(log.Fields{
							"loop": l.loopConfig.LoopName,
							"ts":   ts,
						}).Debug("backfill has caught up")
						ctx.Stop(l.factory.baseConfig.ApplyTimeout)

						// Add a testing hook to validate backfill
						// behaviors before switching to consistent
						// mode.
						if ch := l.loopConfig.WaitAfterBackfill; ch != nil {
							log.Info("waiting to continue after backfill")
							select {
							case <-ch:
								log.Info("continuing after backfill")
							case <-ctx.Done():
								return ctx.Err()
							}
						}
						return nil
					}
				} else if canBackfill {
					if delta > l.factory.baseConfig.BackfillWindow {
						log.WithFields(log.Fields{
							"loop": l.loopConfig.LoopName,
							"ts":   ts,
						}).Warn("replication has fallen behind, switching to backfill mode")
						ctx.Stop(l.factory.baseConfig.ApplyTimeout)
						return nil
					}
				}

				// Wait for the consistent point to advance, for
				// shutdown, or for cancellation.
				select {
				case <-pointChanged:
					point, pointChanged = l.consistentPoint.Get()
				case <-ctx.Stopping():
					// Graceful shutdown.
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	// Wait for graceful initialize or cancellation.
	return errors.Wrapf(ctx.Wait(), "loop %s", l.loopConfig.LoopName)
}

type fillFn = func(context.Context, chan<- Message, State) error

// chooseFillStrategy returns the strategy that will be used for
// generating replication messages.
func (l *loop) chooseFillStrategy() (choice fillFn, events Events, isBackfill bool) {
	choice = l.loopConfig.Dialect.ReadInto
	if l.factory.baseConfig.Immediate {
		events = l.events.fan
	} else {
		events = l.events.serial
	}
	// Is backfilling supported?
	back, ok := l.loopConfig.Dialect.(Backfiller)
	if !ok {
		return
	}
	// Is backfilling enabled by the user?
	if l.factory.baseConfig.BackfillWindow <= 0 {
		return
	}
	// Is the last consistent point sufficiently old to backfill?
	cp, _ := l.consistentPoint.Get()
	ts, ok := cp.(TimeStamp)
	if !ok {
		return
	}
	delta := time.Since(ts.AsTime())
	if delta < l.factory.baseConfig.BackfillWindow {
		return
	}
	choice = back.BackfillInto
	events = l.events.fan
	isBackfill = true
	log.WithFields(log.Fields{
		"delta": delta,
		"loop":  l.loopConfig.LoopName,
		"ts":    ts,
	}).Debug("using backfill strategy")
	return
}

// doBackfill provides the implementation of Events.Backfill.
func (l *loop) doBackfill(loopName string, backfiller Backfiller) error {
	// Create a copy of the individual loop configuration, with an
	// updated name.
	cfg := l.loopConfig.Copy()
	cfg.Dialect = backfiller
	cfg.LoopName = loopName

	// Create a nested stopper.
	stop := stopper.WithContext(l.running)
	// We don't need any grace time since the sub-loop has exited.
	defer func() { stop.Stop(0) }()

	filler, err := l.factory.newLoop(stop, cfg)
	if err != nil {
		return err
	}

	return filler.loop.runOnceUsing(
		stop,
		backfiller.BackfillInto,
		filler.loop.events.fan,
		true /* isBackfilling */)
}

// storeConsistentPoint commits the given stamp to the memo table.
func (l *loop) storeConsistentPoint(p stamp.Stamp) error {
	data, err := json.Marshal(p)
	if err != nil {
		return errors.WithStack(err)
	}
	return l.factory.memo.Put(context.Background(),
		l.factory.stagingPool, l.loopConfig.LoopName, data,
	)
}

// stopperEvents exposes the iteration stop signal to the Dialect.
type stopperEvents struct {
	Events
	stopping <-chan struct{}
}

// Stopping returns the per-iteration stop signal.
func (s *stopperEvents) Stopping() <-chan struct{} {
	return s.stopping
}
