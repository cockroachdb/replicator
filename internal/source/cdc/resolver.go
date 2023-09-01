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

package cdc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Schema declared here for ease of reference, but it's actually created
// in the factory.
//
// The secondary index allows us to find the last-known resolved
// timestamp for a target schema without running into locks held by a
// dequeued timestamp. This revscan is necessary to ensure ordering
// invariants when marking new timestamps.
const schema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_schema     STRING  NOT NULL,
  source_nanos      INT     NOT NULL,
  source_logical    INT     NOT NULL,
  target_applied_at TIMESTAMP,
  PRIMARY KEY (target_schema, source_nanos, source_logical),
  INDEX (target_schema, source_nanos DESC, source_logical DESC)    
)`

// A resolver is an implementation of a logical.Dialect which records
// incoming resolved timestamps and (asynchronously) applies them.
// Resolver instances are created for each destination schema.
type resolver struct {
	cfg         *Config
	fastWakeup  chan struct{}
	leases      types.Leases
	loop        *logical.Loop // Reference to driving loop, for testing.
	pool        *types.StagingPool
	retirements chan hlc.Time // Drives a goroutine to remove applied mutations.
	stagers     types.Stagers
	target      ident.Schema
	watcher     types.Watcher

	sql struct {
		mark            string
		record          string
		selectTimestamp string
	}
}

var (
	_ logical.Backfiller         = (*resolver)(nil)
	_ logical.ConsistentCallback = (*resolver)(nil)
	_ logical.Dialect            = (*resolver)(nil)
	_ logical.Lessor             = (*resolver)(nil)
)

func newResolver(
	ctx context.Context,
	cfg *Config,
	leases types.Leases,
	pool *types.StagingPool,
	metaTable ident.Table,
	stagers types.Stagers,
	target ident.Schema,
	watchers types.Watchers,
) (*resolver, error) {
	watcher, err := watchers.Get(ctx, target)
	if err != nil {
		return nil, err
	}

	ret := &resolver{
		cfg:         cfg,
		fastWakeup:  make(chan struct{}, 1),
		leases:      leases,
		pool:        pool,
		retirements: make(chan hlc.Time, 16),
		stagers:     stagers,
		target:      target,
		watcher:     watcher,
	}
	ret.sql.selectTimestamp = fmt.Sprintf(selectTimestampTemplate, metaTable)
	ret.sql.mark = fmt.Sprintf(markTemplate, metaTable)
	ret.sql.record = fmt.Sprintf(recordTemplate, metaTable)

	return ret, nil
}

// Acquire a lease for the destination schema.
func (r *resolver) Acquire(ctx context.Context) (types.Lease, error) {
	return r.leases.Acquire(ctx, r.target.Raw())
}

// This query conditionally inserts a new mark for a target schema if
// there is no previous mark or if the proposed mark is after the
// latest-known mark for the target schema.
//
// $1 = target_schema
// $2 = source_nanos
// $3 = source_logical
const markTemplate = `
WITH
not_before AS (
  SELECT source_nanos, source_logical FROM %[1]s
  WHERE target_schema=$1
  ORDER BY source_nanos desc, source_logical desc
  FOR UPDATE
  LIMIT 1),
to_insert AS (
  SELECT $1::STRING, $2::INT, $3::INT
  WHERE (SELECT count(*) FROM not_before) = 0
     OR ($2::INT, $3::INT) > (SELECT (source_nanos, source_logical) FROM not_before))
INSERT INTO %[1]s (target_schema, source_nanos, source_logical)
SELECT * FROM to_insert`

func (r *resolver) Mark(ctx context.Context, ts hlc.Time) error {
	tag, err := r.pool.Exec(ctx,
		r.sql.mark,
		r.target.Schema().Raw(),
		// The sql driver gets the wrong type back from CRDB v20.2
		fmt.Sprintf("%d", ts.Nanos()),
		fmt.Sprintf("%d", ts.Logical()),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	if tag.RowsAffected() == 0 {
		log.Tracef("ignoring no-op resolved timestamp %s", ts)
		return nil
	}
	log.WithFields(log.Fields{
		"schema":   r.target,
		"resolved": ts,
	}).Trace("marked new resolved timestamp")

	r.wake()

	return nil
}

// BackfillInto implements logical.Backfiller.
func (r *resolver) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return r.readInto(ctx, ch, state, true)
}

// ReadInto implements logical.Dialect. It incrementally applies batches
// of mutations within a resolved timeslice to be committed within a
// single transaction.
//
// The dequeuing query uses a SELECT FOR UPDATE NOWAIT query to block
// other instances of cdc-sink that may be running. The transaction will
// be committed when the logical loop's consistent point advances.
func (r *resolver) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return r.readInto(ctx, ch, state, false)
}

func (r *resolver) readInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State, backfill bool,
) error {
	// Resume deletions on restart.
	r.retirements <- state.GetConsistentPoint().(*resolvedStamp).CommittedTime

	const dbPollInterval = 100 * time.Millisecond

	for {
		prev := state.GetConsistentPoint().(*resolvedStamp)
		proposed, err := r.nextProposedStamp(ctx, prev, backfill)

		switch err {
		case nil:
			// We have work to do, send it down the channel.
			select {
			case ch <- proposed:
			case <-state.Stopping():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}

			// Wait for the proposed stamp to be processed before
			// reading the next stamp.
			select {
			case <-state.NotifyConsistentPoint(ctx, logical.AwaitGTE, proposed):
				continue
			case <-state.Stopping():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}

		case errNoWork:
			// We've consumed all available work, become idle.
			select {
			case <-r.fastWakeup:
				// Triggered by a calls to Mark and to OnConsistent.
			case <-time.After(dbPollInterval):
				// Backup polling interval.
			case <-state.Stopping():
				// Clean shutdown
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}

		default:
			return err
		}
	}
}

// nextProposedStamp will load the next unresolved timestamp from the
// database and return a proposed resolvedStamp.   The returned error
// may be one of the sentinel values errNoWork or errBlocked that are
// returned by selectTimestamp.
func (r *resolver) nextProposedStamp(
	ctx context.Context, prev *resolvedStamp, backfill bool,
) (*resolvedStamp, error) {
	// Find the next resolved timestamp to apply, starting from
	// a timestamp known to be committed.
	nextResolved, err := r.selectTimestamp(ctx, prev.CommittedTime)
	if err != nil {
		return nil, err
	}

	// Create a new marker that verifies that we're rolling forward.
	ret, err := prev.NewProposed(nextResolved)
	if err != nil {
		return nil, err
	}

	// Propagate the backfilling flag. This could already be set if
	if backfill {
		ret.Backfill = true
	}

	return ret, nil
}

// Process implements logical.Dialect. It receives a resolved timestamp
// from ReadInto and drains the associated mutations.
func (r *resolver) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	for msg := range ch {
		if logical.IsRollback(msg) {
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
			continue
		}

		if _, err := r.process(ctx, msg.(*resolvedStamp), events); err != nil {
			return err
		}
	}

	return nil
}

const recordTemplate = `
UPSERT INTO %s (target_schema, source_nanos, source_logical, target_applied_at)
VALUES ($1, $2, $3, now())`

// Record is a no-op version of Mark. It simply records an incoming
// resolved timestamp as though it had been processed by the resolver.
// This is used when running in immediate mode to allow resolved
// timestamps from the source cluster to be logged for performance
// analysis and cross-cluster reconciliation via AOST queries.
func (r *resolver) Record(ctx context.Context, ts hlc.Time) error {
	_, err := r.pool.Exec(ctx,
		r.sql.record,
		r.target.Schema().Raw(),
		ts.Nanos(),
		ts.Logical(),
	)
	return errors.WithStack(err)
}

// OnConsistent implements logical.ConsistentCallback. It is called once
// the enclosing logical loop has advanced to a new consistent point. We
// know that it's safe to commit the transaction which has drained the
// various datasources.  Once this transaction is closed, it will unlock
// the resolved-timestamp table, allowing us (or another instance) to
// loop around. Should the drain transaction fail to commit, we would
// retry the same collections of mutations on the next loop.
func (r *resolver) OnConsistent(cp stamp.Stamp) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rs := cp.(*resolvedStamp)
	// Commit the transaction marking the resolved timestamp as complete.
	if rs.ProposedTime == hlc.Zero() {
		if err := r.Record(ctx, rs.CommittedTime); err != nil {
			return errors.Wrap(err, "could not commit resolved-timestamp update")
		}
		log.Tracef("committed resolvedStamp transaction for %s", rs)

		// Retire mutations as the committed time advances.
		r.retirements <- rs.CommittedTime
	}

	// Kick the drain loop.
	r.wake()
	return nil
}

// ZeroStamp implements logical.Dialect.
func (r *resolver) ZeroStamp() stamp.Stamp {
	return &resolvedStamp{}
}

// process makes incremental progress in fulfilling the given
// resolvedStamp. It returns the state to which the resolved timestamp
// has been advanced.
func (r *resolver) process(
	ctx context.Context, rs *resolvedStamp, events logical.Events,
) (*resolvedStamp, error) {
	start := time.Now()
	targets := r.watcher.Get().Order

	cursor := &types.SelectManyCursor{
		Backfill:    rs.Backfill,
		End:         rs.ProposedTime,
		Limit:       r.cfg.SelectBatchSize,
		OffsetKey:   rs.OffsetKey,
		OffsetTable: rs.OffsetTable,
		OffsetTime:  rs.OffsetTime,
		Start:       rs.CommittedTime,
		Targets:     targets,
	}

	flush := func(toApply *ident.TableMap[[]types.Mutation]) error {
		flushStart := time.Now()

		ctx, cancel := context.WithTimeout(ctx, r.cfg.ApplyTimeout)
		defer cancel()

		// Apply the retrieved data.
		if err := events.OnBegin(ctx, rs); err != nil {
			return err
		}

		for _, tables := range targets {
			for _, table := range tables {
				muts := toApply.GetZero(table)
				if len(muts) == 0 {
					continue
				}

				if err := events.OnData(ctx, table.Table(), table, muts); err != nil {
					return err
				}
			}
		}
		// Note that the data isn't necessarily committed to the database
		// at this point. We need to wait for a call to OnConsistent to
		// know that the data is safe.
		if err := events.OnCommit(ctx); err != nil {
			return err
		}

		// Advance the stamp once the flush has completed. The flush
		// cycle can add some intermediate checkpoints for FK ordering,
		// so we don't want to truly advance the ratchet until we know
		// that everything's been committed.
		rs = rs.NewProgress(cursor)

		log.WithFields(log.Fields{
			"duration": time.Since(flushStart),
			"schema":   r.target,
		}).Debugf("flushed mutations")
		return nil
	}

	var epoch hlc.Time
	flushCounter := 0
	toApply := &ident.TableMap[[]types.Mutation]{}
	total := 0
	if err := r.stagers.SelectMany(ctx, r.pool, cursor,
		func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
			// Check for flush before accumulating.
			var needsFlush bool
			if rs.Backfill {
				// We're receiving data in table-order. Just read data
				// and flush it once we hit our soft limit, since we
				// can resume at any point.
				needsFlush = flushCounter >= r.cfg.IdealFlushBatchSize
			} else if r.cfg.FlushEveryTimestamp {
				// The user wants to preserve all intermediate updates
				// to a row, rather than fast-forwarding the values of a
				// row to the latest transactionally-consistent state.
				// We'll flush on every MVCC boundary change.
				needsFlush = epoch != hlc.Zero() && hlc.Compare(mut.Time, epoch) > 0
			} else {
				// We're receiving data ordered by MVCC timestamp. Flush
				// data when we see a new epoch after accumulating a
				// minimum number of mutations. This increases
				// throughput when there are many single-row
				// transactions in the source database.
				needsFlush = flushCounter >= r.cfg.IdealFlushBatchSize &&
					hlc.Compare(mut.Time, epoch) > 0
			}
			if needsFlush {
				if err := flush(toApply); err != nil {
					return err
				}
				total += flushCounter
				flushCounter = 0
				// Reset the slices in toApply; flush() ignores zero-length entries.
				_ = toApply.Range(func(tbl ident.Table, muts []types.Mutation) error {
					toApply.Put(tbl, muts[:0])
					return nil
				})
			}

			flushCounter++
			toApply.Put(tbl, append(toApply.GetZero(tbl), mut))
			epoch = mut.Time
			return nil
		}); err != nil {
		return rs, err
	}

	// Final flush cycle to commit the final stamp.
	rs, err := rs.NewCommitted()
	if err != nil {
		return rs, err
	}
	if err := flush(toApply); err != nil {
		return rs, err
	}
	total += flushCounter
	log.WithFields(log.Fields{
		"committed": rs.CommittedTime,
		"count":     total,
		"duration":  time.Since(start),
		"schema":    r.target,
	}).Debugf("processed resolved timestamp")
	return rs, nil
}

// $1 target_schema
// $2 last_known_nanos
// $3 last_known_logical
const selectTimestampTemplate = `
SELECT source_nanos, source_logical
  FROM %[1]s
 WHERE target_schema=$1
   AND (source_nanos, source_logical)>=($2, $3)
   AND target_applied_at IS NULL
 ORDER BY source_nanos, source_logical
 LIMIT 1
`

// These error values are returned by dequeueInTx to indicate why
// no timestamp was returned.
var (
	errNoWork = errors.New("no work")
)

// selectTimestamp locates the next unresolved timestamp to flush. The after
// parameter allows us to skip over most of the table contents.
func (r *resolver) selectTimestamp(ctx context.Context, after hlc.Time) (hlc.Time, error) {
	var sourceNanos int64
	var sourceLogical int
	if err := r.pool.QueryRow(ctx,
		r.sql.selectTimestamp,
		r.target.Schema().Raw(),
		after.Nanos(),
		after.Logical(),
	).Scan(&sourceNanos, &sourceLogical); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.WithField("schema", r.target).Trace("no work")
			return hlc.Zero(), errNoWork
		}
		return hlc.Zero(), errors.WithStack(err)
	}
	return hlc.New(sourceNanos, sourceLogical), nil
}

// retireLoop starts goroutines to ensure that old mutations are
// eventually discarded. This method will return immediately. The
// goroutines which it spawns will terminate when the context is
// canceled.
func (r *resolver) retireLoop(ctx context.Context) {
	var toRetire atomic.Pointer[hlc.Time]
	wakeup := make(chan struct{}, 1)

	// Coalesce incoming candidates value. This goroutine will exit
	// when the enclosing context is canceled.
	go func() {
		defer close(wakeup)
		for {
			select {
			case retire := <-r.retirements:
				toRetire.Store(&retire)
				select {
				case wakeup <- struct{}{}:
				default:
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// Perform the work, taking however much time is needed. This loop
	// will exit when the wakeup channel has been closed.
	go func() {
		for range wakeup {
			next := *toRetire.Load()
			log.Tracef("retiring mutations in %s <= %s", r.target, next)

			targetTables := r.watcher.Get().Columns
			if err := targetTables.Range(func(table ident.Table, _ []types.ColData) error {
				stager, err := r.stagers.Get(ctx, table)
				if err != nil {
					// Don't log, just stop if canceled.
					if errors.Is(err, context.Canceled) {
						return err
					}
					// Warn, but keep running.
					log.WithError(err).Warnf("could not acquire stager for %s", table)
					return nil
				}
				if err := stager.Retire(ctx, r.pool, next); err != nil {
					if errors.Is(err, context.Canceled) {
						return err
					}
					log.WithError(err).Warnf("error while retiring staged mutations in %s", table)
				}
				return nil
			}); err != nil {
				// We only see an error on cancellation, so just exit.
				return
			}
		}
	}()
}

// wake triggers a nonblocking send to the wakeup channel.
func (r *resolver) wake() {
	select {
	case r.fastWakeup <- struct{}{}:
	default:
	}
}

// Resolvers is a factory for Resolver instances.
type Resolvers struct {
	cfg       *Config
	leases    types.Leases
	loops     *logical.Factory
	noStart   bool // Set by test code to disable call to loop.Start()
	metaTable ident.Table
	pool      *types.StagingPool
	stagers   types.Stagers
	watchers  types.Watchers

	mu struct {
		sync.Mutex
		cleanups  []func()
		instances *ident.SchemaMap[*resolver]
	}
}

// close will drain any running resolver loops.
func (r *Resolvers) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Cancel each loop.
	for _, cancel := range r.mu.cleanups {
		cancel()
	}
	// Wait for shutdown.
	_ = r.mu.instances.Range(func(_ ident.Schema, r *resolver) error {
		<-r.loop.Stopped()
		return nil
	})
	r.mu.cleanups = nil
	r.mu.instances = nil
}

func (r *Resolvers) get(ctx context.Context, target ident.Schema) (*resolver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if found, ok := r.mu.instances.Get(target); ok {
		return found, nil
	}

	ret, err := newResolver(ctx, r.cfg, r.leases, r.pool, r.metaTable, r.stagers, target, r.watchers)
	if err != nil {
		return nil, err
	}

	// For testing, we sometimes want to drive the resolver directly.
	if r.noStart {
		return ret, nil
	}

	loop, cleanup, err := r.loops.Start(&logical.LoopConfig{
		Dialect:      ret,
		LoopName:     "changefeed-" + target.Raw(),
		TargetSchema: target,
	})
	if err != nil {
		return nil, err
	}
	ret.loop = loop

	r.mu.instances.Put(target, ret)

	// Start a goroutine to retire old data.
	retireCtx, cancelRetire := context.WithCancel(context.Background())
	ret.retireLoop(retireCtx)

	r.mu.cleanups = append(r.mu.cleanups, cleanup, cancelRetire)
	return ret, nil
}

const scanForTargetTemplate = `
SELECT DISTINCT target_schema
FROM %[1]s
WHERE target_applied_at IS NULL
`

// ScanForTargetSchemas is used by the factory to ensure that any schema
// with unprocessed, resolved timestamps will have an associated resolve
// instance. This function is declared here to keep the sql queries in a
// single file. It is exported to aid in testing.
func ScanForTargetSchemas(
	ctx context.Context, db types.StagingQuerier, metaTable ident.Table,
) ([]ident.Schema, error) {
	rows, err := db.Query(ctx, fmt.Sprintf(scanForTargetTemplate, metaTable))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	var ret []ident.Schema
	for rows.Next() {
		var schemaRaw string
		if err := rows.Scan(&schemaRaw); err != nil {
			return nil, errors.WithStack(err)
		}

		sch, err := ident.ParseSchema(schemaRaw)
		if err != nil {
			return nil, err
		}

		ret = append(ret, sch)
	}

	return ret, nil
}
