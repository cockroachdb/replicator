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
// the requested value or until the context is cancelled..
func (l *Loop) AwaitConsistentPoint(ctx context.Context, point stamp.Stamp) (stamp.Stamp, error) {
	return l.loop.AwaitConsistentPoint(ctx, point)
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
	memo types.Memo
	// Tracks when it is time to update the consistentPoint.
	standbyDeadline time.Time
	stopped         chan struct{}
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

	if time.Now().Before(l.standbyDeadline) {
		return nil
	}
	if err := l.storeConsistentPoint(p); err != nil {
		return errors.Wrap(err, "could not persistent consistent point")
	}
	l.standbyDeadline = time.Now().Add(l.config.StandbyTimeout)
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

// AwaitConsistentPoint blocks until the consistent point is greater
// than or equal to the given stamp or until the context is cancelled.
// The consistent point that matches the condition will be returned.
func (l *loop) AwaitConsistentPoint(ctx context.Context, point stamp.Stamp) (stamp.Stamp, error) {
	// Fast-path
	if found := l.GetConsistentPoint(); stamp.Compare(found, point) >= 0 {
		return found, nil
	}

	// Use a separate goroutine to allow cancellation.
	result := make(chan stamp.Stamp, 1)
	go func() {
		defer close(result)
		l.consistentPoint.L.Lock()
		defer l.consistentPoint.L.Unlock()
		for {
			found := l.consistentPoint.stamp
			if stamp.Compare(found, point) >= 0 {
				select {
				case result <- found:
				case <-ctx.Done():
				}
				return
			}
			l.consistentPoint.Wait()
		}
	}()

	select {
	case found := <-result:
		return found, nil
	case <-ctx.Done():
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
		// Determine how to perform the filling.
		source, events, isBackfilling := l.chooseFillStrategy()
		err := l.runOnce(ctx, source, events, isBackfilling)

		// If the outer context is done, just return.
		if ctx.Err() != nil {
			log.Tracef("loop %s outer context done", l.config.LoopName)
			return
		}

		// Clean exit, likely switching modes.
		if err == nil {
			continue
		}

		// Otherwise, log the error, and sleep for a bit.
		log.WithError(err).Errorf("error in replication loop %s; retrying in %s",
			l.config.LoopName, l.config.RetryDelay)
		select {
		case <-time.After(l.config.RetryDelay):
		case <-ctx.Done():
			return
		}
	}
}

// runOnce is called by run or by doBackfill.
func (l *loop) runOnce(
	ctx context.Context, source fillFn, events Events, isBackfilling bool,
) error {
	if isBackfilling {
		l.metrics.backfillStatus.Set(1)
	} else {
		l.metrics.backfillStatus.Set(0)
	}

	// Ensure that we're in a clear state when recovering.
	defer events.stop()

	groupCtx, cancelGroup := context.WithCancel(ctx)
	defer cancelGroup()
	group, groupCtx := errgroup.WithContext(groupCtx)

	// Start a background goroutine to maintain the replication
	// connection. This source goroutine is set up to be robust; if
	// there's an error talking to the source database, we send a
	// rollback message to the consumer and retry the connection.
	ch := make(chan Message, 16)
	group.Go(func() error {
		defer close(ch)
		for groupCtx.Err() == nil {
			err := source(groupCtx, ch, events)

			// Return if the source closed cleanly (we may switch
			// from backfill to streaming modes) or if the outer
			// context is being shut down.
			if err == nil || groupCtx.Err() != nil {
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
			case <-groupCtx.Done():
				return nil
			}
		}
		return nil
	})

	// This goroutine applies the incoming mutations to the target
	// database. It is fragile, when it errors, we need to also
	// restart the source goroutine.
	group.Go(func() error {
		err := l.dialect.Process(groupCtx, ch, events)

		// As above.
		if err == nil || groupCtx.Err() != nil {
			return nil
		}

		log.WithError(err).Errorf(
			"error while applying replication messages %s; stopping",
			l.config.LoopName)
		return err
	})

	// If we're in backfill mode, start a goroutine to stop the backfill
	// process once we've caught up. This routine never errors out, and
	// we don't need to wait for it below.
	if isBackfilling {
		go func() {
			defer cancelGroup()

			l.consistentPoint.L.Lock()
			defer l.consistentPoint.L.Unlock()
			for groupCtx.Err() == nil {
				ts := l.consistentPoint.stamp.(TimeStamp).AsTime()
				if time.Since(ts) < l.config.BackfillWindow {
					log.WithFields(log.Fields{
						"loop": l.config.LoopName,
						"ts":   ts,
					}).Debug("backfill has caught up")
					return
				}
				l.consistentPoint.Wait()
			}
		}()

		// When the group context has closed, we want to perform a
		// broadcast on the consistent point, to ensure that the above
		// goroutine doesn't become blocked on the call to Wait().
		go func() {
			<-groupCtx.Done()
			l.consistentPoint.L.Lock()
			defer l.consistentPoint.L.Unlock()
			l.consistentPoint.Broadcast()
		}()
	}

	groupErr := group.Wait()
	// Ensure that the latest point has been saved on a clean exit.
	if groupErr == nil {
		if err := l.storeConsistentPoint(l.GetConsistentPoint()); err != nil {
			log.WithError(err).Warnf("could not save consistent point for %s", l.config.LoopName)
		}
	}
	return groupErr
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

	return filler.loop.runOnce(ctx, backfiller.BackfillInto, filler.loop.events.fan, true)
}
