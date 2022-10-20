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
	"math/rand"
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

// tableOffset records partial progress when backfilling a table.
type tableOffset struct {
	Key  json.RawMessage `json:"k"`
	Time hlc.Time        `json:"t"`
}

// resolvedStamp tracks the progress of applying staged mutations for
// a given resolved timestamp.  This type has some extra complexity to
// support backfilling of initial changefeed scans.
type resolvedStamp struct {
	// A resolved timestamp that represents a transactionally-consistent
	// point in the history of the workload.
	CommittedTime hlc.Time `json:"c,omitempty"`
	// FKLevel is used to skip ahead when backfilling foreign-keyed
	// data. We ensure that we arrive at a consistent point between
	// each FK level, so that fan-mode will correctly sequence updates.
	FKLevel int `json:"f,omitempty"`
	// Iteration is incremented on each loop through a partial backfill
	// when trying to drain at a particular time. This allows each
	// batch to have a well-ordered stamp.
	Iteration int `json:"i,omitempty"`
	// Offsets memoize the last key and time that was drained from any
	// given table, allowing backfills to page through the pending data.
	Offsets map[string]tableOffset `json:"o,omitempty"`
	// A timestamp
	ProposedTime hlc.Time `json:"p,omitempty"`

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

// IsBackfill returns true if the iteration counter is non-zero.
func (s *resolvedStamp) IsBackfill() bool {
	return s.Iteration != 0
}

// Less implements stamp.Stamp.
func (s *resolvedStamp) Less(other stamp.Stamp) bool {
	o := other.(*resolvedStamp)
	if c := hlc.Compare(s.CommittedTime, o.CommittedTime); c != 0 {
		return c < 0
	}
	return s.Iteration-o.Iteration < 0
}

// NewCommitted returns a new resolvedStamp that represents a committed
// version of the proposed time.
func (s *resolvedStamp) NewCommitted() *resolvedStamp {
	if s.ProposedTime == hlc.Zero() {
		panic("cannot make new committed timestamp without proposed value")
	}
	return &resolvedStamp{
		CommittedTime: s.ProposedTime,
		Tx:            s.Tx,
	}
}

// NewProposed returns a new resolvedStamp that propagates information into
// the next loop.
func (s *resolvedStamp) NewProposed(tx pgx.Tx, proposed hlc.Time, backfill bool) *resolvedStamp {
	work := &resolvedStamp{
		CommittedTime: s.CommittedTime,
		ProposedTime:  proposed,
		Tx:            tx,
	}
	// Copy incremental work into next iteration.
	if backfill {
		work.Iteration = s.Iteration + 1
		work.FKLevel = s.FKLevel
		work.Offsets = make(map[string]tableOffset, len(s.Offsets))
		for k, v := range s.Offsets {
			work.Offsets[k] = v
		}
	}
	return work
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
		requeue string
	}
}

var (
	_ logical.Backfiller         = (*resolver)(nil)
	_ logical.ConsistentCallback = (*resolver)(nil)
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
	ret.sql.requeue = fmt.Sprintf(requeueTemplate, metaTable)

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

// BackfillInto implements logical.Backfiller. It imposes a maximum
// batch size on the number of rows within a (potentially massive)
// resolved timeslice to operate on.
func (r *resolver) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return r.readInto(ctx, ch, true, state)
}

// ReadInto implements logical.Dialect. It applies all mutations within
// a resolved timeslice to be committed within a single transaction.
func (r *resolver) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return r.readInto(ctx, ch, false, state)
}

// readInto will open a new transaction and dequeue a resolved
// timestamp. The dequeuing query uses a SELECT FOR UPDATE NOWAIT query
// to block other instances of cdc-sink that may be running. The
// transaction will be committed when the logical loop's consistent
// point advances.
func (r *resolver) readInto(
	ctx context.Context, ch chan<- logical.Message, backfill bool, state logical.State,
) error {
	// Resume deletions on restart.
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
			work := prev.NewProposed(tx, nextResolved, backfill)

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
			// Exit backfilling mode when there's no work to do.
			_ = tx.Rollback(ctx)
			if backfill {
				return nil
			}
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
			rs.Rollback()
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

func (r *resolver) process(ctx context.Context, rs *resolvedStamp, events logical.Events) error {
	allBatches := make(map[ident.Table][][]types.Mutation)
	limit := r.cfg.BackfillBatchSize // Only used when backfilling.
	requeue := false
	sawData := false
	targetTables := r.watcher.Snapshot(r.target).Order[rs.FKLevel:]

	// Dequeue and apply the mutations for each table in their
	// dependency group.
drain:
	for _, tables := range targetTables {
		// Shuffle the table order. In a backfill situation, we can
		// help to spread out the load. This slice is a copy, so it's
		// safe to mutate.
		rand.Shuffle(len(tables), func(i, j int) {
			tables[i], tables[j] = tables[j], tables[i]
		})
		for _, table := range tables {
			stager, err := r.stagers.Get(ctx, table)
			if err != nil {
				return err
			}
			var muts []types.Mutation
			if rs.IsBackfill() {
				// Use the previous page's timestamp, or fall back to
				// the last-known committed time.
				backfillStart := rs.Offsets[table.Raw()].Time
				if backfillStart == hlc.Zero() {
					backfillStart = rs.CommittedTime
				}

				muts, err = stager.SelectPartial(ctx,
					rs.Tx,
					backfillStart,
					rs.ProposedTime,
					rs.Offsets[table.Raw()].Key, // Skip over previously-read rows.
					limit,
				)
			} else {
				muts, err = stager.Select(ctx, rs.Tx, rs.CommittedTime, rs.ProposedTime)
			}
			if err != nil {
				return err
			}
			if len(muts) == 0 {
				continue
			}

			sawData = true
			allBatches[table] = append(allBatches[table], muts)

			if rs.IsBackfill() {
				// Ensure we can pick up where we left off.
				lastMut := muts[len(muts)-1]
				rs.Offsets[table.Raw()] = tableOffset{
					Key:  lastMut.Key,
					Time: lastMut.Time,
				}

				// If we hit our limit during the backfill, requeue the
				// resolved timestamp and exit from this loop. This
				// allows the loop to checkpoint progress along the way.
				limit -= len(muts)
				if limit <= 0 {
					requeue = true
					break drain
				}
			}
		}

		// If we're backfilling and have reached the end of a level, add
		// a consistent point that represents all tables having been
		// filled at the current level. When we restart, we'll skip
		// ahead into the ordered targetTables slice.
		if rs.IsBackfill() && sawData && len(targetTables) > 1 {
			rs.FKLevel++
			requeue = true
			break drain
		}
	}

	// Requeue a resolved timestamp if there's a partial batch,
	// otherwise we clear the backfill timestamp to exit from
	// backfilling mode.
	if requeue {
		if err := r.requeueInTx(ctx, rs.Tx, rs.ProposedTime); err != nil {
			return err
		}
	} else {
		// We've entirely finished a resolved timestamp, so mark the
		// proposed time as committed.
		rs = rs.NewCommitted()
	}

	// Start applying the retrieved data.
	if err := events.OnBegin(ctx, rs); err != nil {
		return err
	}

	for table, batches := range allBatches {
		for _, batch := range batches {
			if err := events.OnData(ctx, table.Table(), table, batch); err != nil {
				return err
			}
		}
	}

	// Note that the data isn't necessarily committed to the database
	// at this point. We need to wait for a call to OnConsistent to
	// know that the data is safe.
	return events.OnCommit(ctx)
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

	if err := rs.Commit(ctx); err != nil {
		return errors.Wrap(err, "could not commit resolved-timestamp update")
	}
	log.Tracef("committed resolvedStamp transaction for %s", rs)

	// Retire mutations once the backfill is complete.
	if !rs.IsBackfill() {
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

// This query is executed during a partial backfill, when there are
// mutations left unprocessed.
const requeueTemplate = `
UPDATE %[1]s
   SET target_applied_at = NULL
 WHERE target_db=$1
   AND target_schema=$2
   AND source_nanos=$3
   AND source_logical=$4
   AND target_applied_at IS NOT NULL
`

// requeue marks the given timestamp as being eligible for
// reprocessing. We execute this method at the end of a partial backfill
// cycle.
func (r *resolver) requeueInTx(ctx context.Context, tx pgxtype.Querier, ts hlc.Time) error {
	tag, err := tx.Exec(ctx,
		r.sql.requeue,
		r.target.Database().Raw(),
		r.target.Schema().Raw(),
		ts.Nanos(),
		ts.Logical())
	if err != nil {
		return errors.WithStack(err)
	}
	if tag.RowsAffected() != 1 {
		return errors.Errorf("did not requeue timestamp: %d rows affected", tag.RowsAffected())
	}
	return nil
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
