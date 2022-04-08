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

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/serial"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// impl provides a common feature set for processing single-stream,
// logical replication feeds. This can be used to build feeds from any
// data source which has well-defined "consistent points", such as
// write-ahead-log offsets or explicit transaction ids.
//
// impl is not internally synchronized; it assumes that it is being
// driven by a serial stream of data.
type impl struct {
	// Locked on mu.
	consistentPointUpdated *sync.Cond
	// The Dialect contains message-processing, specific to a particular
	// source database.
	dialect Dialect
	// The fan manages the fan-out of applying mutations across multiple
	// SQL connections.
	fan *fan.Fan
	// openTransaction tracks the latest value passed to OnCommit.
	openTransaction stamp.Stamp
	// The amount of time to sleep between retries of the replication
	// loop.
	retryDelay time.Duration
	// Allows us to force the concurrent applier logic to concentrate
	// its work into a single underlying database transaction. This will
	// be nil when running in the default, concurrent, mode.
	serializer *serial.Pool
	// The SQL database we're going to be writing into.
	targetDB ident.Ident

	mu struct {
		sync.Mutex

		// This represents a position in the source's transaction log.
		// It is subject to a mutex, since we receive the notifications
		// from the Fan in an asynchronous manner.
		consistentPoint stamp.Stamp
	}
}

// Start constructs a new replication loop that will process messages
// until the context is cancelled. The returned channel can be monitored
// to know when the loop has halted.
func Start(
	ctx context.Context, config *Config, dialect Dialect,
) (stopped <-chan struct{}, _ error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}

	// Bring up connection to target database.
	targetCfg, err := pgxpool.ParseConfig(config.TargetConn)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse %q", config.TargetConn)
	}
	// Identify traffic.
	targetCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET application_name=$1", "cdc-sink")
		return err
	}
	// Bigger pools are better for CRDB.
	targetCfg.MaxConns = int32(config.TargetDBConns)
	// Ensure connection diversity through long-lived loadbalancers.
	targetCfg.MaxConnLifetime = 10 * time.Minute
	// Keep one spare connection.
	targetCfg.MinConns = 1

	targetPool, err := pgxpool.ConnectConfig(ctx, targetCfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to CockroachDB")
	}

	watchers, cancelWatchers := schemawatch.NewWatchers(targetPool)
	appliers, cancelAppliers := apply.NewAppliers(watchers)

	loop := &impl{
		dialect:    dialect,
		retryDelay: config.RetryDelay,
		targetDB:   config.TargetDB,
		mu: struct {
			sync.Mutex
			consistentPoint stamp.Stamp
		}{},
	}
	loop.consistentPointUpdated = sync.NewCond(&loop.mu)

	// If the user wants to preserve transaction boundaries, we inject a
	// helper which forces all database operations to be applied within
	// a single transaction.
	var applyQuerier pgxtype.Querier = targetPool
	applyShards := 16
	if !config.Immediate {
		loop.serializer = &serial.Pool{Pool: targetPool}
		applyQuerier = loop.serializer
		applyShards = 1
	}
	var cancelFan func()
	loop.fan, cancelFan, err = fan.New(
		appliers,
		config.ApplyTimeout,
		applyQuerier,
		loop.setConsistentPoint,
		applyShards,
		config.BytesInFlight,
	)
	if err != nil {
		return nil, err
	}

	stopper := make(chan struct{})
	go func() {
		loop.run(ctx)
		cancelFan()
		cancelAppliers()
		cancelWatchers()
		targetPool.Close()
		close(stopper)
	}()

	return stopper, nil
}

// GetConsistentPoint implements State.
func (l *impl) GetConsistentPoint() stamp.Stamp {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.consistentPoint
}

// GetTargetDB implements State.
func (l *impl) GetTargetDB() ident.Ident {
	return l.targetDB
}

// OnBegin implements Events.
func (l *impl) OnBegin(ctx context.Context, point stamp.Stamp) error {
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
func (l *impl) OnCommit(ctx context.Context) error {
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
	return err
}

// OnData implements Events.
func (l *impl) OnData(ctx context.Context, target ident.Table, muts []types.Mutation) error {
	return l.fan.Enqueue(ctx, l.openTransaction, target, muts)
}

// OnRollback implements Events.
func (l *impl) OnRollback(ctx context.Context, msg Message) error {
	if !IsRollback(msg) {
		return errors.New("the rollback message must be passed to OnRollback")
	}
	l.reset()
	return nil
}

// reset is called before every attempt at running the replication loop.
func (l *impl) reset() {
	l.fan.Reset()
	l.openTransaction = nil
	if s := l.serializer; s != nil {
		// Don't really care about the transaction state.
		_ = s.Rollback(context.Background())
	}
}

// run blocks while the connection is processing messages.
func (l *impl) run(ctx context.Context) {
	for ctx.Err() == nil {
		// Ensure that we're in a clear state when recovering.
		l.reset()
		group, ctx := errgroup.WithContext(ctx)

		// Start a background goroutine to maintain the replication
		// connection. This source goroutine is set up to be robust; if
		// there's an error talking to the source database, we send a
		// rollback message to the consumer and retry the connection.
		ch := make(chan Message, 16)
		group.Go(func() error {
			defer close(ch)
			for {
				if err := l.dialect.ReadInto(ctx, ch, l); err != nil {
					log.WithError(err).Error("error from replication source; continuing")
				}
				// If the context was canceled, just exit.
				if err := ctx.Err(); err != nil {
					return nil
				}
				// Otherwise, we'll recover by injecting a new rollback
				// message and then restarting the message stream from
				// the previous consistent point.
				select {
				case ch <- msgRollback:
					continue
				case <-ctx.Done():
					return nil
				}
			}
		})

		// This goroutine applies the incoming mutations to the target
		// database. It is fragile, when it errors, we need to also
		// restart the source goroutine.
		group.Go(func() error {
			err := l.dialect.Process(ctx, ch, l)
			if err != nil {
				log.WithError(err).Error("error while applying replication messages; stopping")
			}
			return err
		})

		if err := group.Wait(); err != nil {
			log.WithError(err).Errorf("error in replication loop; retrying in %s", l.retryDelay)
			select {
			case <-ctx.Done():
			case <-time.After(l.retryDelay):
			}
		}
	}
	log.Info("shut down replication loop")
}

func (l *impl) setConsistentPoint(p stamp.Stamp) {
	l.mu.Lock()
	l.mu.consistentPoint = p
	l.mu.Unlock()
	l.consistentPointUpdated.Broadcast()
}
