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

	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/serial"
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
	// the key used to persist the consistentPoint stamp.
	consistentPointKey string
	// Locked on mu.
	consistentPointUpdated *sync.Cond
	// The Dialect contains message-processing, specific to a particular
	// source database.
	dialect Dialect
	// The fan manages the fan-out of applying mutations across multiple
	// SQL connections.
	fan *fan.Fan
	// Optional checkpoint saved into the target database
	memo types.Memo
	// openTransaction tracks the latest value passed to OnCommit.
	openTransaction stamp.Stamp
	// The amount of time to sleep between retries of the replication
	// loop.
	retryDelay time.Duration
	// Allows us to force the concurrent applier logic to concentrate
	// its work into a single underlying database transaction. This will
	// be nil when running in the default, concurrent, mode.
	serializer *serial.Pool
	// Tracks when it is time to update the consistentPoint.
	standbyDeadline time.Time
	stopped         chan struct{}
	// The SQL database we're going to be writing into.
	targetDB ident.Ident
	// Used to update the consistentPoint in the target database.
	targetPool pgxtype.Querier

	mu struct {
		sync.Mutex

		// This represents a position in the source's transaction log.
		// It is subject to a mutex, since we receive the notifications
		// from the Fan in an asynchronous manner.
		consistentPoint stamp.Stamp
	}
}

var standbyTimeout = 5 * time.Second

func (l *loop) saveConsistentPoint(ctx context.Context) error {
	if l.memo == nil {
		return nil
	}
	m, err := l.GetConsistentPoint().MarshalText()
	if err != nil {
		return err
	}
	log.Infof("Saving checkpoint %s", string(m))
	return l.memo.Put(ctx, l.targetPool, l.consistentPointKey, m)
}

func (l *loop) retrieveConsistentPoint(
	ctx context.Context, memo types.Memo, sourceID string, defaultValue []byte,
) ([]byte, error) {
	if memo == nil {
		return []byte(""), nil
	}
	return memo.Get(ctx, l.targetPool, sourceID, defaultValue)
}

// GetConsistentPoint implements State.
func (l *loop) GetConsistentPoint() stamp.Stamp {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.consistentPoint
}

// GetTargetDB implements State.
func (l *loop) GetTargetDB() ident.Ident {
	return l.targetDB
}

// OnBegin implements Events.
func (l *loop) OnBegin(ctx context.Context, point stamp.Stamp) error {
	if l.openTransaction != nil {
		return errors.Errorf("OnBegin already called at %s", l.openTransaction)
	}
	l.openTransaction = point
	if l.serializer == nil {
		return nil
	}
	return l.serializer.Begin(ctx)
}

// OnCommit implements Events.
func (l *loop) OnCommit(ctx context.Context) error {
	if l.openTransaction == nil {
		return errors.New("OnCommit called without matching OnBegin")
	}

	var err error
	defer func() {
		tx := l.openTransaction
		l.openTransaction = nil
		if err != nil {
			commitFailureCount.Inc()
		} else {
			commitSuccessCount.Inc()
			if x, ok := tx.(TimeStamp); ok {
				commitTime.Set(float64(x.AsTime().UnixNano()))
			}
			if x, ok := tx.(OffsetStamp); ok {
				commitOffset.Set(float64(x.AsOffset()))
			}
		}
	}()

	if err = l.fan.Mark(l.openTransaction); err != nil {
		return err
	}

	// If we're running in serial (as opposed to concurrent) mode, we
	// want to wait for the pending mutations to be flushed to the
	// single transaction, and then we'll commit the transaction.
	if l.serializer != nil {
		l.mu.Lock()
		for stamp.Compare(l.mu.consistentPoint, l.openTransaction) < 0 {
			l.consistentPointUpdated.Wait()
		}
		l.mu.Unlock()
		// err is checked by the deferred call above.
		err = l.serializer.Commit(ctx)
	}

	if time.Now().After(l.standbyDeadline) {
		err = l.saveConsistentPoint(ctx)
		if err != nil {
			return err
		}
		l.standbyDeadline = time.Now().Add(standbyTimeout)
	}
	return err
}

// OnData implements Events.
func (l *loop) OnData(ctx context.Context, target ident.Table, muts []types.Mutation) error {
	return l.fan.Enqueue(ctx, l.openTransaction, target, muts)
}

// OnRollback implements Events.
func (l *loop) OnRollback(_ context.Context, msg Message) error {
	if !IsRollback(msg) {
		return errors.New("the rollback message must be passed to OnRollback")
	}
	l.reset()
	return nil
}

// reset is called before every attempt at running the replication loop.
func (l *loop) reset() {
	l.fan.Reset()
	l.openTransaction = nil
	if s := l.serializer; s != nil {
		// Don't really care about the transaction state.
		_ = s.Rollback(context.Background())
	}
}

// run blocks while the connection is processing messages.
func (l *loop) run(ctx context.Context) {
	defer log.Info("replication loop shut down")
	for {
		// Ensure that we're in a clear state when recovering.
		l.reset()
		group, groupCtx := errgroup.WithContext(ctx)

		// Start a background goroutine to maintain the replication
		// connection. This source goroutine is set up to be robust; if
		// there's an error talking to the source database, we send a
		// rollback message to the consumer and retry the connection.
		ch := make(chan Message, 16)
		group.Go(func() error {
			defer close(ch)
			for {
				if err := l.dialect.ReadInto(groupCtx, ch, l); err != nil {
					log.WithError(err).Error("error from replication source; continuing")
				}
				// If the context was canceled, just exit.
				if err := groupCtx.Err(); err != nil {
					return nil
				}
				// Otherwise, we'll recover by injecting a new rollback
				// message and then restarting the message stream from
				// the previous consistent point.
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
			err := l.dialect.Process(groupCtx, ch, l)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.WithError(err).Error("error while applying replication messages; stopping")
			}
			return err
		})

		err := group.Wait()

		// If the outer context is done, just return.
		if ctx.Err() != nil {
			return
		}

		log.WithError(err).Errorf("error in replication loop; retrying in %s", l.retryDelay)
		select {
		case <-time.After(l.retryDelay):
		case <-ctx.Done():
			return
		}
	}
}

func (l *loop) setConsistentPoint(p stamp.Stamp) {
	l.mu.Lock()
	l.mu.consistentPoint = p
	l.mu.Unlock()
	l.consistentPointUpdated.Broadcast()
}
