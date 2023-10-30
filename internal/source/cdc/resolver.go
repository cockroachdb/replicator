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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
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
	leases      types.Leases
	marked      notify.Var[hlc.Time] // Called by Mark.
	pool        *types.StagingPool
	retirements notify.Var[hlc.Time] // Drives a goroutine to remove applied mutations.
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
	_ logical.Backfiller = (*resolver)(nil)
	_ logical.Dialect    = (*resolver)(nil)
	_ logical.Lessor     = (*resolver)(nil)
)

func newResolver(
	cfg *Config,
	leases types.Leases,
	pool *types.StagingPool,
	metaTable ident.Table,
	stagers types.Stagers,
	target ident.Schema,
	watchers types.Watchers,
) (*resolver, error) {
	watcher, err := watchers.Get(target)
	if err != nil {
		return nil, err
	}

	ret := &resolver{
		cfg:     cfg,
		leases:  leases,
		pool:    pool,
		stagers: stagers,
		target:  target,
		watcher: watcher,
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

	r.marked.Set(ts)

	return nil
}

// BackfillInto implements logical.Backfiller.
func (r *resolver) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return r.readInto(ctx, ch, state)
}

// ReadInto implements logical.Dialect.
func (r *resolver) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return r.readInto(ctx, ch, state)
}

func (r *resolver) readInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	// This will either be from a previous iteration or ZeroStamp.
	cp, cpUpdated := state.GetConsistentPoint()
	resumeFrom := cp.(*resolvedStamp)

	// Resume deletions on restart.
	if resumeFrom.CommittedTime != hlc.Zero() {
		r.retirements.Set(resumeFrom.CommittedTime)
	}

	// See discussion on BackupPolling field. A timer is preferred to
	// time.After() since After always creates a new goroutine.
	backupTimer := time.NewTimer(r.cfg.BackupPolling)
	defer backupTimer.Stop()

	// Internal notification path when Mark is called.
	_, wakeup := r.marked.Get()
	for {
		if resumeFrom != nil {
			var toSend *resolvedStamp

			// If we're resuming from an in-progress checkpoint, just
			// send it. This covers the case where cdc-sink was
			// restarted in the middle of a backfill.
			if resumeFrom.ProposedTime != hlc.Zero() {
				log.WithFields(log.Fields{
					"loop":       r.target,
					"resumeFrom": resumeFrom,
				}).Trace("loop resuming from partial progress")

				toSend = resumeFrom
				resumeFrom = nil
			} else {
				// We're looping around with a committed timestamp, so
				// look for work to do and send it.
				proposed, err := r.nextProposedStamp(ctx, resumeFrom)
				switch {
				case err == nil:
					log.WithFields(log.Fields{
						"loop":       r.target,
						"proposed":   proposed,
						"resumeFrom": resumeFrom,
					}).Trace("loop advancing from consistent")

					toSend = proposed
					resumeFrom = nil

				case errors.Is(err, errNoWork):
					// OK, just wait for something to happen.

				default:
					return err
				}
			}

			// Send new timestamp.
			if toSend != nil {
				select {
				case ch <- toSend:
				case <-state.Stopping():
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		// Drain timer channel and reset.
		backupTimer.Stop()
		select {
		case <-backupTimer.C:
		default:
		}
		backupTimer.Reset(r.cfg.BackupPolling)

		select {
		case <-cpUpdated:
			// When the consistent point has advanced to a committed
			// state (i.e. we're not in a partially-backfilled state),
			// update the resume-from point to look for the next
			// resolved timestamp.
			cp, cpUpdated = state.GetConsistentPoint()
			if next, ok := cp.(*resolvedStamp); ok && next.ProposedTime == hlc.Zero() {
				resumeFrom = next
				// Allow applied mutations to be deleted.
				r.retirements.Set(next.CommittedTime)
				log.WithFields(log.Fields{
					"loop":       r.target,
					"resumeFrom": resumeFrom,
				}).Trace("loop is resuming")
			}
		case <-wakeup:
			// Triggered when Mark() adds a new unresolved timestamp.
			_, wakeup = r.marked.Get()
		case <-backupTimer.C:
			// Looks for work added by other cdc-sink instances.
		case <-state.Stopping():
			// Clean shutdown
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// nextProposedStamp will load the next unresolved timestamp from the
// database and return a proposed resolvedStamp.   The returned error
// may be one of the sentinel values errNoWork or errBlocked that are
// returned by selectTimestamp.
func (r *resolver) nextProposedStamp(
	ctx context.Context, prev *resolvedStamp,
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

	return ret, nil
}

// Process implements logical.Dialect. It receives a resolved timestamp
// from ReadInto and drains the associated mutations.
func (r *resolver) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	for msg := range ch {
		if logical.IsRollback(msg) {
			// We don't have any state that needs to be cleaned up.
			continue
		}

		if err := r.process(ctx, msg.(*resolvedStamp), events); err != nil {
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

// ZeroStamp implements logical.Dialect.
func (r *resolver) ZeroStamp() stamp.Stamp {
	return &resolvedStamp{}
}

// process makes incremental progress in fulfilling the given
// resolvedStamp.
func (r *resolver) process(ctx context.Context, rs *resolvedStamp, events logical.Events) error {
	start := time.Now()
	targets := r.watcher.Get().Order

	if len(targets) == 0 {
		return errors.Errorf("no tables known in schema %s; have they been created?", r.target)
	}

	flattened := make([]ident.Table, 0, len(targets))
	for _, tgts := range targets {
		flattened = append(flattened, tgts...)
	}

	cursor := &types.UnstageCursor{
		StartAt:        rs.CommittedTime,
		EndBefore:      rs.ProposedTime,
		Targets:        flattened,
		TimestampLimit: r.cfg.TimestampWindowSize,
		UpdateLimit:    r.cfg.LargeTransactionLimit,
	}
	if rs.LargeBatchOffset != nil && rs.LargeBatchOffset.Len() > 0 {
		rs.LargeBatchOffset.CopyInto(&cursor.StartAfterKey)
	}

	flush := func(toApply *ident.TableMap[[]types.Mutation]) error {
		flushStart := time.Now()

		ctx, cancel := context.WithTimeout(ctx, r.cfg.ApplyTimeout)
		defer cancel()

		if err := retry.Retry(ctx, func(ctx context.Context) error {
			// Apply the retrieved data.
			batch, err := events.OnBegin(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = batch.OnRollback(ctx) }()

			for _, tables := range targets {
				for _, table := range tables {
					muts := toApply.GetZero(table)
					if len(muts) == 0 {
						continue
					}
					source := script.SourceName(r.target)
					muts = append([]types.Mutation(nil), muts...)
					if err := batch.OnData(ctx, source, table, muts); err != nil {
						return err
					}
				}
			}

			// OnCommit is asynchronous to support (a future)
			// unified-immediate mode. We'll wait for the data to be
			// committed.
			select {
			case err := <-batch.OnCommit(ctx):
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"duration": time.Since(flushStart),
			"schema":   r.target,
		}).Debugf("flushed mutations")

		return nil
	}

	total := 0 // Reported in logging at the bottom of this method.

	// We run in a loop until the attempt to unstage returns no more
	// data (i.e. we've consumed all mutations within the resolving
	// window). If a LargeBatchLimit is configured, we may wind up
	// calling flush() several times within a particularly large MVCC
	// timestamp.
	for hadData := true; hadData; {
		if err := retry.Retry(ctx, func(ctx context.Context) error {
			unstageTX, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return errors.Wrap(err, "could not open staging transaction")
			}
			defer func() { _ = unstageTX.Rollback(ctx) }()

			var epoch hlc.Time
			var nextCursor *types.UnstageCursor
			pendingMutations := 0
			toApply := &ident.TableMap[[]types.Mutation]{}

			nextCursor, hadData, err = r.stagers.Unstage(ctx, unstageTX, cursor,
				func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
					total++

					// Decorate the mutation.
					script.AddMeta("cdc", tbl, &mut)

					if epoch == hlc.Zero() {
						epoch = mut.Time
					} else if r.cfg.FlushEveryTimestamp ||
						pendingMutations >= r.cfg.IdealFlushBatchSize {
						// If the user wants to see all intermediate
						// values for a row, we'll flush whenever
						// there's a change in the timestamp value. We
						// may also preemptively flush to prevent
						// unbounded memory use.
						if hlc.Compare(mut.Time, epoch) > 0 {
							if err := flush(toApply); err != nil {
								return err
							}
							epoch = mut.Time
							pendingMutations = 0
							toApply = &ident.TableMap[[]types.Mutation]{}
						}
					} else if hlc.Compare(mut.Time, epoch) < 0 {
						// This would imply a coding error in Unstage.
						return errors.New("epoch going backwards")
					}

					// Accumulate the mutation.
					toApply.Put(tbl, append(toApply.GetZero(tbl), mut))
					pendingMutations++
					return nil
				})
			if err != nil {
				return errors.Wrap(err, "could not unstage mutations")
			}

			if hadData {
				// Final flush of the data that was unstaged. It's
				// possible, although unlikely, that the final callback
				// from Unstage performed a flush. If this were to
				// happen, we don't want to perform a no-work flush.
				if pendingMutations > 0 {
					if err := flush(toApply); err != nil {
						return err
					}
				}

				// Commit the transaction that unstaged the data. Note that
				// without an X/A transaction model, there's always a
				// possibility where we've committed the target transaction
				// and then the unstaging transaction fails to commit. In
				// general, we expect that re-applying a mutation within a
				// short timeframe should be idempotent, although this might
				// not necessarily be the case for data-aware merge
				// functions in the absence of conflict-free datatypes. We
				// could choose to keep a manifest of (table, key,
				// timestamp) entries in the target database to filter
				// repeated updates.
				// https://github.com/cockroachdb/cdc-sink/issues/565
				if err := unstageTX.Commit(ctx); err != nil {
					return errors.Wrap(err, "could not commit unstaging transaction; "+
						"mutations may be reapplied")
				}

				// If we're going to continue around, update the
				// consistent point with partial progress. This ensures
				// that cdc-sink can make progress on large batches,
				// even if it's restarted.
				cursor = nextCursor
				rs = rs.NewProgress(cursor)
			} else {
				// We read no data, which means that we're done. We'll
				// let the unstaging transaction be rolled back by the
				// defer above since it performed no actual work in the
				// database.
				rs, err = rs.NewCommitted()
				if err != nil {
					return err
				}
				// Mark the timestamp has being processed.
				if err := r.Record(ctx, rs.CommittedTime); err != nil {
					return err
				}
			}

			// Save the consistent point to be able to resume if the
			// cdc-sink process is stopped.
			if err := events.SetConsistentPoint(ctx, rs); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	log.WithFields(log.Fields{
		"committed": rs.CommittedTime,
		"count":     total,
		"duration":  time.Since(start),
		"schema":    r.target,
	}).Debugf("processed resolved timestamp")
	return nil
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
			return hlc.Zero(), errNoWork
		}
		return hlc.Zero(), errors.WithStack(err)
	}
	return hlc.New(sourceNanos, sourceLogical), nil
}

// retireLoop starts a goroutine to ensure that old mutations are
// eventually discarded. This method will return immediately.
func (r *resolver) retireLoop(ctx *stopper.Context) {
	ctx.Go(func() error {
		next, nextUpdated := r.retirements.Get()
		for {
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
				// Retain staged data for an extra amount of time.
				if off := r.cfg.RetireOffset; off > 0 {
					next = hlc.New(next.Nanos()-off.Nanoseconds(), next.Logical())
				}
				if err := stager.Retire(ctx, r.pool, next); err != nil {
					if errors.Is(err, context.Canceled) {
						return err
					}
					log.WithError(err).Warnf("error while retiring staged mutations in %s", table)
				}
				return nil
			}); err != nil {
				log.WithError(err).Warnf("error in retire loop for %s", r.target)
			}

			select {
			case <-nextUpdated:
				next, nextUpdated = r.retirements.Get()
			case <-ctx.Stopping():
				return nil
			}
		}
	})
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
	stop      *stopper.Context // Manage lifecycle of background processes.
	watchers  types.Watchers

	mu struct {
		sync.Mutex
		instances *ident.SchemaMap[*logical.Loop]
	}
}

// get creates or returns the [logical.Loop] and the enclosed resolver.
func (r *Resolvers) get(target ident.Schema) (*logical.Loop, *resolver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if found, ok := r.mu.instances.Get(target); ok {
		return found, found.Dialect().(*resolver), nil
	}

	ret, err := newResolver(r.cfg, r.leases, r.pool, r.metaTable, r.stagers, target, r.watchers)
	if err != nil {
		return nil, nil, err
	}

	// For testing, we sometimes want to drive the resolver directly.
	if r.noStart {
		return nil, ret, nil
	}

	loop, err := r.loops.Start(&logical.LoopConfig{
		Dialect:      ret,
		LoopName:     "changefeed-" + target.Raw(),
		TargetSchema: target,
	})
	if err != nil {
		return nil, nil, err
	}

	r.mu.instances.Put(target, loop)

	// Start a goroutine to retire old data.
	ret.retireLoop(r.stop)

	return loop, ret, nil
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
