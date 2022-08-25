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
	loop *loop
}

// Dialect returns the logical.Dialect in use.
func (l *Loop) Dialect() Dialect {
	return l.loop.dialect
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
func (l *loop) setConsistentPoint(p stamp.Stamp) {
	l.consistentPoint.L.Lock()
	defer l.consistentPoint.L.Unlock()
	l.consistentPoint.stamp = p
	l.consistentPoint.Broadcast()

	if l.config.LoopName == "" {
		return
	}
	if time.Now().Before(l.standbyDeadline) {
		return
	}
	data, err := json.Marshal(p)
	if err != nil {
		log.WithError(err).Warn("could not marshal consistent-point stamp")
		return
	}
	if err := l.memo.Put(context.Background(),
		l.targetPool, l.config.LoopName, data,
	); err != nil {
		log.WithError(err).Warn("could not persist consistent-point stamp")
		return
	}
	l.standbyDeadline = time.Now().Add(l.config.StandbyTimeout)
	log.Tracef("Saved checkpoint %s", data)
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
	defer log.Info("replication loop shut down")
	for {
		err := l.runOnce(ctx)

		// If the outer context is done, just return.
		if ctx.Err() != nil {
			return
		}

		// Clean exit, likely switching modes.
		if err == nil {
			continue
		}

		// Otherwise, log the error, and sleep for a bit.
		log.WithError(err).Errorf("error in replication loop; retrying in %s", l.config.RetryDelay)
		select {
		case <-time.After(l.config.RetryDelay):
		case <-ctx.Done():
			return
		}
	}
}

// runOnce is called by run.
func (l *loop) runOnce(ctx context.Context) error {
	// Determine how to perform the filling.
	source, events, isBackfilling := l.chooseFillStrategy()

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
			if err == nil {
				return nil
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}

			// Otherwise, we'll recover by injecting a new rollback
			// message and then restarting the message stream from
			// the previous consistent point.
			log.WithError(err).Error("error from replication source; continuing")
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
		if err != nil && !errors.Is(err, context.Canceled) {
			log.WithError(err).Error("error while applying replication messages; stopping")
		}
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
					}).Info("backfill has caught up")
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
	events = l.events.fan
	isBackfill = true
	// Do we have specific backfilling behavior to call?
	if back, ok := l.dialect.(Backfiller); ok {
		choice = back.BackfillInto
	}
	log.WithFields(log.Fields{
		"delta": delta,
		"loop":  l.config.LoopName,
		"ts":    ts,
	}).Info("using backfill strategy")
	return
}
