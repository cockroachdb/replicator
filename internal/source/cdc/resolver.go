// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// resolvedStamp tracks the progress of applying staged mutations for
// a given resolved timestamp.  This type has some extra complexity to
// support backfilling of initial changefeed scans.
//
// Here's a state diagram to help:
//
//	committed
//	    |                *---*
//	    V                V   |
//	 proposed ---> backfill -*
//	    |                |
//	    V                |
//	committed <----------*
//
// Tracking of FK levels and table offsets is only used in backfill mode
// and is especially relevant to an initial_scan from a changefeed. In
// this state, we have an arbitrarily large number of mutations to
// apply, all of which will have the same timestamp.
type resolvedStamp struct {
	Backfill bool `json:"b,omitempty"`
	// A resolved timestamp that represents a transactionally-consistent
	// point in the history of the workload.
	CommittedTime hlc.Time `json:"c,omitempty"`
	// Iteration is used to provide well-ordered behavior within a
	// single backfill window.
	Iteration int `json:"i,omitempty"`
	// The next resolved timestamp that we want to advance to.
	ProposedTime hlc.Time `json:"p,omitempty"`

	OffsetKey   json.RawMessage `json:"ok,omitempty"`
	OffsetTable ident.Table     `json:"otbl,omitempty"`
	OffsetTime  hlc.Time        `json:"ots,omitempty"`

	// Tx holds a reference to a transaction that, when committed,
	// marks the resolved timestamp as having been processed.
	Tx interface {
		pgxtype.Querier
		Commit(context.Context) error
		Rollback(context.Context) error
	} `json:"-"`
}

// AsTime implements logical.TimeStamp to improve reporting.
func (s *resolvedStamp) AsTime() time.Time {
	// Use the older time when backfilling.
	return time.Unix(0, s.CommittedTime.Nanos())
}

// Commit marks the resolved timestamp as resolved.
func (s *resolvedStamp) Commit(ctx context.Context) error {
	tx := s.Tx
	if tx == nil {
		return errors.New("transaction already ended")
	}
	s.Tx = nil
	return tx.Commit(ctx)
}

// Less implements stamp.Stamp.
func (s *resolvedStamp) Less(other stamp.Stamp) bool {
	o := other.(*resolvedStamp)
	if c := hlc.Compare(s.CommittedTime, o.CommittedTime); c != 0 {
		return c < 0
	}
	return s.Iteration < o.Iteration
}

// NewCommitted returns a new resolvedStamp that represents the
// completion of a resolved timestamp.
func (s *resolvedStamp) NewCommitted() *resolvedStamp {
	if s.ProposedTime == hlc.Zero() {
		panic(errors.New("cannot make new committed timestamp without proposed value"))
	}
	return &resolvedStamp{
		CommittedTime: s.ProposedTime,
		Tx:            s.Tx,
	}
}

// NewProposed returns a new resolvedStamp that extends the existing
// stamp with a later proposed timestamp.
func (s *resolvedStamp) NewProposed(tx pgx.Tx, proposed hlc.Time) *resolvedStamp {
	if hlc.Compare(proposed, s.CommittedTime) <= 0 {
		panic(errors.Errorf("proposed time must advance: %s vs %s", proposed, s.CommittedTime))
	}
	if hlc.Compare(proposed, s.OffsetTime) < 0 {
		panic(errors.Errorf("proposed time undoing work: %s vs %s", proposed, s.OffsetTime))
	}
	if hlc.Compare(proposed, s.ProposedTime) < 0 {
		panic(errors.Errorf("proposed time cannot go backward: %s vs %s", proposed, s.ProposedTime))
	}

	return &resolvedStamp{
		Backfill:      s.Backfill,
		CommittedTime: s.CommittedTime,
		Iteration:     s.Iteration + 1,
		OffsetKey:     s.OffsetKey,
		OffsetTable:   s.OffsetTable,
		OffsetTime:    s.OffsetTime,
		ProposedTime:  proposed,
		Tx:            tx,
	}

}

// NewBackfill returns a resolvedStamp that represents partial progress
// within the same [committed, proposed] window.
func (s *resolvedStamp) NewBackfill(cursor *types.SelectManyCursor) *resolvedStamp {
	ret := &resolvedStamp{
		Backfill:      true,
		CommittedTime: s.CommittedTime,
		Iteration:     s.Iteration + 1,
		OffsetKey:     s.OffsetKey,
		OffsetTable:   s.OffsetTable,
		OffsetTime:    s.OffsetTime,
		ProposedTime:  s.ProposedTime,
		Tx:            s.Tx,
	}
	if cursor != nil {
		ret.OffsetKey = cursor.OffsetKey
		ret.OffsetTable = cursor.OffsetTable
		ret.OffsetTime = cursor.OffsetTime
	}
	return ret
}

// Rollback aborts the enclosed transaction, if it exists. This method
// is safe to call on a nil receiver.
func (s *resolvedStamp) Rollback() {
	if s == nil {
		return
	}
	tx := s.Tx
	if tx == nil {
		return
	}
	s.Tx = nil
	_ = tx.Rollback(context.Background())
	log.Tracef("rolled back a resolvedStamp")
}

// String is for debugging use only.
func (s *resolvedStamp) String() string {
	ret, _ := json.Marshal(s)
	return string(ret)
}

// Schema declared here for ease of reference, but it's actually created
// in the factory.
//
// The secondary index allows us to find the last-known resolved
// timestamp for a target schema without running into locks held by a
// dequeued timestamp. This revscan is necessary to ensure ordering
// invariants when marking new timestamps.
const schema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_db         STRING  NOT NULL,
  target_schema     STRING  NOT NULL,
  source_nanos      INT     NOT NULL,
  source_logical    INT     NOT NULL,
  target_applied_at TIMESTAMP,
  PRIMARY KEY (target_db, target_schema, source_nanos, source_logical),
  INDEX (target_db, target_schema, source_nanos DESC, source_logical DESC)    
)`

// A resolver is an implementation of a logical.Dialect which records
// incoming resolved timestamps and (asynchronously) applies them.
// Resolver instances are created for each destination schema.
type resolver struct {
	cfg         *Config
	fastWakeup  chan struct{}
	loop        *logical.Loop // Reference to driving loop, for testing.
	pool        *pgxpool.Pool
	retirements chan hlc.Time // Drives a goroutine to remove applied mutations.
	stagers     types.Stagers
	target      ident.Schema
	watcher     types.Watcher

	sql struct {
		dequeue string
		mark    string
	}
}

var (
	_ logical.Backfiller         = (*resolver)(nil)
	_ logical.ConsistentCallback = (*resolver)(nil)
	_ logical.Dialect            = (*resolver)(nil)
)

func newResolver(
	ctx context.Context,
	cfg *Config,
	pool *pgxpool.Pool,
	metaTable ident.Table,
	stagers types.Stagers,
	target ident.Schema,
	watchers types.Watchers,
) (*resolver, error) {
	watcher, err := watchers.Get(ctx, target.Database())
	if err != nil {
		return nil, err
	}

	ret := &resolver{
		cfg:         cfg,
		fastWakeup:  make(chan struct{}, 1),
		pool:        pool,
		retirements: make(chan hlc.Time, 16),
		stagers:     stagers,
		target:      target,
		watcher:     watcher,
	}
	ret.sql.dequeue = fmt.Sprintf(dequeueTemplate, metaTable)
	ret.sql.mark = fmt.Sprintf(markTemplate, metaTable)

	return ret, nil
}

// This query conditionally inserts a new mark for a target schema if
// there is no previous mark or if the proposed mark is after the
// latest-known mark for the target schema.
//
// $1 = target_db
// $2 = target_schema
// $3 = source_nanos
// $4 = source_logical
const markTemplate = `
WITH
not_before AS (
  SELECT source_nanos, source_logical FROM %[1]s
  WHERE target_db=$1 AND target_schema=$2
  ORDER BY source_nanos desc, source_logical desc
  LIMIT 1),
to_insert AS (
  SELECT $1::STRING, $2::STRING, $3::INT, $4::INT
  WHERE (SELECT count(*) FROM not_before) = 0
     OR ($3::INT, $4::INT) > (SELECT (source_nanos, source_logical) FROM not_before))
INSERT INTO %[1]s (target_db, target_schema, source_nanos, source_logical)
SELECT * FROM to_insert`

func (r *resolver) Mark(ctx context.Context, ts hlc.Time) error {
	tag, err := r.pool.Exec(ctx,
		r.sql.mark,
		r.target.Database().Raw(),
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
) error { // Resume deletions on restart.
	r.retirements <- state.GetConsistentPoint().(*resolvedStamp).CommittedTime

	const minSleep = 10 * time.Millisecond
	const maxSleep = 1 * time.Second
	const backoff = 10
	sleepDelay := minSleep
	for {
		prev := state.GetConsistentPoint().(*resolvedStamp)

		// This transaction will be committed in OnConsistent.
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		// Find the next resolved timestamp to apply, starting from
		// a timestamp known to be committed.
		nextResolved, err := r.dequeueInTx(ctx, tx, prev.CommittedTime)

		switch err {
		case nil:
			work := prev.NewProposed(tx, nextResolved)
			// If we're in backfill mode and aren't continuing from a
			// previous, partial backfill, mark the work to be performed
			// as a backfill.
			if backfill && !work.Backfill {
				work = work.NewBackfill(nil)
			}

			select {
			case ch <- work:
				// We expect to get a fast wakeup when we finish
				// processing this timestamp, so set the sleep value to
				// a conservative value. This keeps us from trying to
				// SELECT FOR UPDATE the row that we're already
				// processing, but still allows for recovery if
				// something goes off the rails downstream.
				sleepDelay = maxSleep
			case <-ctx.Done():
				_ = tx.Rollback(ctx)
				return ctx.Err()
			}

		case errNoWork:
			// We've consumed all available work, become idle.
			_ = tx.Rollback(ctx)
			sleepDelay = maxSleep

		case errBlocked:
			// Another instance is running, just sleep.
			_ = tx.Rollback(ctx)

			// If we had a fast wakeup and are immediately blocked,
			// start an exponential backoff.
			if sleepDelay < maxSleep {
				sleepDelay *= backoff
				if sleepDelay > maxSleep {
					sleepDelay = maxSleep
				}
			}

		default:
			_ = tx.Rollback(ctx)
			return err
		}

		select {
		case <-r.fastWakeup:
			// Triggered by a calls to Mark and to OnConsistent.
			sleepDelay = minSleep
		case <-time.After(sleepDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Process implements logical.Dialect. It receives a resolved timestamp
// from ReadInto and drains the associated mutations.
func (r *resolver) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	// Track the most recently seen stamp so that we can clean up if
	// we see a rollback message.
	var rs *resolvedStamp

	for msg := range ch {
		if logical.IsRollback(msg) {
			// Abort the transaction that would have marked the resolved
			// timestamp as having been processed. This will unlock the
			// read loop to allow an instance of cdc-sink to retry the
			// timestamp.
			if rs != nil {
				rs.Rollback()
			}
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
			continue
		}

		rs = msg.(*resolvedStamp)
		if err := r.process(ctx, rs, events); err != nil {
			rs.Rollback()
			return err
		}
	}
	return nil
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
		if err := rs.Commit(ctx); err != nil {
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

func (r *resolver) process(ctx context.Context, rs *resolvedStamp, events logical.Events) error {
	start := time.Now()
	targets := r.watcher.Snapshot(r.target).Order
	total := 0

	flush := func(toApply map[ident.Table][]types.Mutation) (int, error) {
		flushCount := 0
		flushStart := time.Now()

		// Send a command on this transaction to prevent the
		// idle-in-transaction timeout from kicking in.
		if _, err := rs.Tx.Exec(ctx, "SELECT 1"); err != nil {
			return 0, errors.Wrap(err, "could not ping resolved-timestamp transaction")
		}

		// Apply the retrieved data.
		if err := events.OnBegin(ctx, rs); err != nil {
			return 0, err
		}

		// Push the data in FK-preserving order. If we cross an FK group
		// boundary in backfilling mode, insert a new resolvedStamp to
		// ensure that parent tables are flushed before child tables.
		lastGroup := 0
		for group, tables := range targets {
			for _, table := range tables {
				muts := toApply[table]
				if len(muts) == 0 {
					continue
				}

				if rs.Backfill && group > lastGroup {
					lastGroup = group
					// Close previous group.
					if err := events.OnCommit(ctx); err != nil {
						return 0, err
					}
					// Increment stamp.
					rs = rs.NewBackfill(nil)
					// Start new group.
					if err := events.OnBegin(ctx, rs); err != nil {
						return 0, err
					}
				}

				flushCount += len(muts)
				if err := events.OnData(ctx, table.Table(), table, muts); err != nil {
					return 0, err
				}
			}
		}
		// Note that the data isn't necessarily committed to the database
		// at this point. We need to wait for a call to OnConsistent to
		// know that the data is safe.
		if err := events.OnCommit(ctx); err != nil {
			return 0, err
		}
		log.WithFields(log.Fields{
			"count":    flushCount,
			"duration": time.Since(flushStart),
			"schema":   r.target,
		}).Debugf("flushed mutations")
		return flushCount, nil
	}

	cursor := &types.SelectManyCursor{
		Backfill:    rs.Backfill,
		End:         rs.ProposedTime,
		Limit:       r.cfg.IdealMinBatchSize,
		OffsetKey:   rs.OffsetKey,
		OffsetTable: rs.OffsetTable,
		OffsetTime:  rs.OffsetTime,
		Start:       rs.CommittedTime,
		Targets:     targets,
	}

	for {
		toApply, more, err := r.stagers.SelectMany(ctx, r.pool, cursor)
		if err != nil {
			return err
		}
		count, err := flush(toApply)
		if err != nil {
			return err
		}
		total += count

		// Advance the stamp once the flush has completed. The flush
		// cycle can add some intermediate checkpoints for FK ordering,
		// so we don't want to truly advance the ratchet until we know
		// that everything's been committed.
		if more {
			rs = rs.NewBackfill(cursor)
		} else {
			break
		}
	}

	// Final flush cycle to commit the final stamp.
	rs = rs.NewCommitted()
	if _, err := flush(nil); err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"count":    total,
		"duration": time.Since(start),
		"schema":   r.target,
		"from":     rs.CommittedTime,
		"resolved": rs.ProposedTime,
	}).Debugf("processed resolved timestamp")
	return nil
}

// The FOR UPDATE NOWAIT will quickly fail the query with a 55P03 code
// if another transaction is processing that timestamp. We use the WHERE
// ... IN clause to ensure a lookup join.
//
// $1 target_db
// $2 target_schema
// $3 last_known_nanos
// $4 last_known_logical
const dequeueTemplate = `
WITH work AS (
 SELECT target_db, target_schema, source_nanos, source_logical
   FROM %[1]s
  WHERE target_db=$1
    AND target_schema=$2
    AND (source_nanos, source_logical)>=($3, $4)
    AND target_applied_at IS NULL
  ORDER BY source_nanos, source_logical
    FOR UPDATE NOWAIT
  LIMIT 1)
UPDATE %[1]s
   SET target_applied_at=now()
 WHERE (target_db, target_schema, source_nanos, source_logical) IN (SELECT * FROM work)
RETURNING source_nanos, source_logical
`

// These error values are returned by dequeueInTx to indicate why
// no timestamp was returned.
var (
	errNoWork  = errors.New("no work")
	errBlocked = errors.New("blocked")
)

// dequeueInTx locates the next unresolved timestamp to flush. The after
// parameter allows us to skip over most of the table contents.
func (r *resolver) dequeueInTx(
	ctx context.Context, tx pgxtype.Querier, after hlc.Time,
) (hlc.Time, error) {
	var sourceNanos int64
	var sourceLogical int
	err := tx.QueryRow(ctx,
		r.sql.dequeue,
		r.target.Database().Raw(),
		r.target.Schema().Raw(),
		after.Nanos(),
		after.Logical(),
	).Scan(&sourceNanos, &sourceLogical)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.WithField("schema", r.target).Trace("no work")
			return hlc.Zero(), errNoWork
		}
		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			log.WithField("schema", r.target).Trace("blocked by other")
			return hlc.Zero(), errBlocked
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
	var toRetire atomic.Value
	wakeup := make(chan struct{}, 1)

	// Coalesce incoming candidates value. This goroutine will exit
	// when the enclosing context is canceled.
	go func() {
		defer close(wakeup)
		for {
			select {
			case retire := <-r.retirements:
				toRetire.Store(retire)
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
			next := toRetire.Load().(hlc.Time)
			log.Tracef("retiring mutations in %s <= %s", r.target, next)

			targetTables := r.watcher.Snapshot(r.target).Columns
			for table := range targetTables {
				stager, err := r.stagers.Get(ctx, table)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.WithError(err).Warnf("could not acquire stager for %s", table)
					continue
				}
				if err := stager.Retire(ctx, r.pool, next); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.WithError(err).Warnf("error while retiring staged mutations in %s", table)
				}
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
	noStart   bool // Set by test code to disable call to loop.Start()
	metaTable ident.Table
	pool      *pgxpool.Pool
	stagers   types.Stagers
	watchers  types.Watchers

	mu struct {
		sync.Mutex
		cleanups  []func()
		instances map[ident.Schema]*resolver
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
	for _, r := range r.mu.instances {
		<-r.loop.Stopped()
	}
	r.mu.cleanups = nil
	r.mu.instances = nil
}

func (r *Resolvers) get(ctx context.Context, target ident.Schema) (*resolver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if found, ok := r.mu.instances[target]; ok {
		return found, nil
	}

	ret, err := newResolver(ctx, r.cfg, r.pool, r.metaTable, r.stagers, target, r.watchers)
	if err != nil {
		return nil, err
	}

	// Start a new logical loop using a fresh configuration.
	cfg := r.cfg.Base().Copy()
	cfg.LoopName = "changefeed-" + target.Raw()
	cfg.TargetDB = target.Database()

	// For testing, we sometimes want to drive the resolver directly.
	if r.noStart {
		return ret, nil
	}

	loop, cleanup, err := logical.Start(ctx, cfg, ret)
	if err != nil {
		return nil, err
	}
	ret.loop = loop

	r.mu.instances[target] = ret

	// Start a goroutine to retire old data.
	retireCtx, cancelRetire := context.WithCancel(context.Background())
	ret.retireLoop(retireCtx)

	r.mu.cleanups = append(r.mu.cleanups, cleanup, cancelRetire)
	return ret, nil
}

const scanForTargetTemplate = `
SELECT DISTINCT target_db, target_schema
FROM %[1]s
WHERE target_applied_at IS NULL
`

// ScanForTargetSchemas is used by the factory to ensure that any schema
// with unprocessed, resolved timestamps will have an associated resolve
// instance. This function is declared here to keep the sql queries in a
// single file. It is exported to aid in testing.
func ScanForTargetSchemas(
	ctx context.Context, db pgxtype.Querier, metaTable ident.Table,
) ([]ident.Schema, error) {
	rows, err := db.Query(ctx, fmt.Sprintf(scanForTargetTemplate, metaTable))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	var ret []ident.Schema
	for rows.Next() {
		var db, schema string
		if err := rows.Scan(&db, &schema); err != nil {
			return nil, errors.WithStack(err)
		}

		ret = append(ret, ident.NewSchema(ident.New(db), ident.New(schema)))
	}

	return ret, nil
}
