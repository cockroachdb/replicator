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
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
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

// Stopped returns a channel that is closed when the Loop has shut down.
func (l *Loop) Stopped() <-chan struct{} {
	return l.loop.stopped
}

// loop is not internally synchronized; it assumes that it is being
// driven by a serial stream of data.
type loop struct {
	config *Config
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
}

// retrieveConsistentPoint will return the latest consistent-point
// stamp, or nil if consistentPointKey is unset.
func (l *loop) retrieveConsistentPoint(ctx context.Context) (stamp.Stamp, error) {
	if l.config.ConsistentPointKey == "" {
		return nil, nil
	}
	data, err := l.memo.Get(ctx, l.targetPool, l.config.ConsistentPointKey)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	return l.dialect.UnmarshalStamp(data)
}

// setConsistentPoint is safe to call from any goroutine. It will
// occasionally persist the consistent point to the memo table.
func (l *loop) setConsistentPoint(p stamp.Stamp) {
	l.consistentPoint.L.Lock()
	defer l.consistentPoint.L.Unlock()
	l.consistentPoint.stamp = p
	l.consistentPoint.Broadcast()

	if l.config.ConsistentPointKey == "" {
		return
	}
	if time.Now().Before(l.standbyDeadline) {
		return
	}
	data, err := p.MarshalText()
	if err != nil {
		log.WithError(err).Warn("could not marshal consistent-point stamp")
		return
	}
	if err := l.memo.Put(context.Background(),
		l.targetPool, l.config.ConsistentPointKey, data,
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

		// Clean exit, likely switching modes.
		if err == nil {
			continue
		}

		// If the outer context is done, just return.
		if ctx.Err() != nil {
			return
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
	// Determine if we should execute in backfill mode.
	var backfiller Backfiller
	if x, ok := l.dialect.(Backfiller); ok && l.config.AllowBackfill && x.ShouldBackfill(l) {
		backfiller = x
		log.Info("using backfill strategy")
	}

	// Select the event-handling strategy.
	events := l.events.serial
	if backfiller != nil || l.config.Immediate {
		events = l.events.fan
	}
	// Ensure that we're in a clear state when recovering.
	defer events.stop()

	group, groupCtx := errgroup.WithContext(ctx)

	// Start a background goroutine to maintain the replication
	// connection. This source goroutine is set up to be robust; if
	// there's an error talking to the source database, we send a
	// rollback message to the consumer and retry the connection.
	ch := make(chan Message, 16)
	group.Go(func() error {
		defer close(ch)
		for {
			var err error
			if backfiller != nil {
				err = backfiller.BackfillInto(groupCtx, ch, events)
			} else {
				err = l.dialect.ReadInto(groupCtx, ch, events)
			}

			// Return if the source closed cleanly (we may switch
			// from backfill to streaming modes) or if the outer
			// context is being shut down.
			if err == nil || errors.Is(err, context.Canceled) {
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

	return group.Wait()
}
