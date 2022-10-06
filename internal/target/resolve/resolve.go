// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package resolve contains a component for recording resolved
// timestamps from a source changefeed and applying the associated
// mutations in an asynchronous manner.
package resolve

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type resolve struct {
	appliers   types.Appliers
	pool       *pgxpool.Pool
	stagers    types.Stagers
	target     ident.Schema
	timekeeper types.TimeKeeper
	watcher    types.Watcher

	// Used for a fast-path wakeup if there's only one instance.
	fastWakeup chan struct{}

	metrics struct {
		errors       prometheus.Counter
		lastCheck    prometheus.Gauge
		lastSuccess  prometheus.Gauge
		lastResolved prometheus.Gauge
		successes    prometheus.Counter
	}

	sql struct {
		dequeue string
		mark    string
	}
}

var _ types.Resolver = (*resolve)(nil)

// Schema declared here for ease of reference, but it's actually created
// in the factory.
const schema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_db         STRING  NOT NULL,
  target_schema     STRING  NOT NULL,
  source_nanos      INT     NOT NULL,
  source_logical    INT     NOT NULL,
  target_applied_at TIMESTAMP,
  PRIMARY KEY (target_db, target_schema, source_nanos, source_logical),
  INDEX (target_db, target_schema, source_nanos, source_logical)
    WHERE target_applied_at IS NULL
)`

func newResolve(
	ctx context.Context,
	appliers types.Appliers,
	metaTable ident.Table,
	pool *pgxpool.Pool,
	stagers types.Stagers,
	target ident.Schema,
	timekeeper types.TimeKeeper,
	watchers types.Watchers,
) (*resolve, error) {
	watcher, err := watchers.Get(ctx, target.Database())
	if err != nil {
		return nil, err
	}

	ret := &resolve{
		appliers:   appliers,
		fastWakeup: make(chan struct{}, 1),
		pool:       pool,
		stagers:    stagers,
		target:     target,
		timekeeper: timekeeper,
		watcher:    watcher,
	}

	labelValues := metrics.SchemaValues(target)
	ret.metrics.errors = resolveFlushErrors.WithLabelValues(labelValues...)
	ret.metrics.lastCheck = resolveLastChecked.WithLabelValues(labelValues...)
	ret.metrics.lastSuccess = resolveLastSuccess.WithLabelValues(labelValues...)
	ret.metrics.lastResolved = resolveLastHLC.WithLabelValues(labelValues...)
	ret.metrics.successes = resolveFlushSuccess.WithLabelValues(labelValues...)

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
  SELECT x.source_nanos, x.source_logical FROM %[1]s x
  WHERE target_db=$1 AND target_schema=$2
  ORDER BY x.source_nanos desc, x.source_logical desc
  LIMIT 1),
to_insert AS (
  SELECT $1::STRING, $2::STRING, $3::INT, $4::INT
  WHERE (SELECT count(*) FROM not_before) = 0
     OR ($3::INT, $4::INT) > (SELECT (source_nanos, source_logical) FROM not_before))
INSERT INTO %[1]s
SELECT * FROM to_insert`

// Mark implements the Resolver interface.
func (r *resolve) Mark(ctx context.Context, tx pgxtype.Querier, next hlc.Time) (bool, error) {
	tag, err := tx.Exec(ctx,
		r.sql.mark,
		r.target.Database().Raw(),
		r.target.Schema().Raw(),
		// The sql driver gets the wrong type back from CRDB v20.2
		fmt.Sprintf("%d", next.Nanos()),
		fmt.Sprintf("%d", next.Logical()),
	)
	ret := tag.RowsAffected() > 0
	if ret {
		log.WithFields(log.Fields{
			"schema":   r.target,
			"resolved": next,
		}).Trace("marked new resolved timestamp")

		// Non-blocking send to local fast-path channel.
		select {
		case r.fastWakeup <- struct{}{}:
		default:
		}
	}
	return ret, errors.WithStack(err)
}

// loop processes resolved timestamps until the context is canceled.
func (r *resolve) loop(ctx context.Context) {
	entry := log.WithField("schema", r.target)
	// Minimize GC spam in the sleep case below.
	timer := time.NewTimer(0)

	for {
		r.metrics.lastCheck.SetToCurrentTime()

		if resolved, didWork, err := r.Flush(ctx); err != nil {
			r.metrics.errors.Inc()
			entry.WithError(err).Warn("could not resolve; will retry")
		} else {
			r.metrics.lastResolved.Set(float64(resolved.Nanos()))
			r.metrics.lastSuccess.SetToCurrentTime()
			r.metrics.successes.Inc()

			// Retry immediately if we did some work.
			if didWork > 0 {
				entry.Tracef("successfully resolved %d mutations", didWork)
				continue
			}
		}

		// Create a variable delay that averages once per second.
		delay := 500*time.Millisecond + time.Duration(rand.Int63n(int64(time.Second)))

		// Reset() can only be called if the timer has been stopped
		// and drained.
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timer.Reset(delay)

		select {
		case <-ctx.Done():
			return
		case <-r.fastWakeup:
		case <-timer.C:
		}
	}
}

// Flush implements the Resolver interface.
func (r *resolve) Flush(ctx context.Context) (hlc.Time, types.FlushDetail, error) {
	var resolved hlc.Time
	var detail types.FlushDetail

	err := retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer tx.Rollback(context.Background())

		resolved, err = r.dequeueInTx(ctx, tx)
		switch err {
		case nil:
			// OK
		case errNoWork:
			detail = types.FlushNoWork
			return nil
		case errBlocked:
			detail = types.FlushBlocked
			return nil
		default:
			return err
		}

		// Load the previously-resolved timestamp.
		prev, err := r.timekeeper.Put(ctx, tx, r.target, resolved)
		if err != nil {
			return err
		}

		if hlc.Compare(resolved, prev) < 0 {
			return errors.Errorf(
				"resolved timestamp went backwards: dequeued %s; had %s",
				resolved, prev)
		}

		log.WithFields(log.Fields{
			"schema": r.target,
			"prev":   prev,
			"next":   resolved,
		}).Trace("attempting to drain")

		// Determine the tables to operate on.
		targetTables := r.watcher.Snapshot(r.target).Columns

		// Set up the per-table helpers.
		appliers := make([]types.Applier, 0, len(targetTables))
		stagers := make([]types.Stager, 0, len(targetTables))
		for table := range targetTables {
			stager, err := r.stagers.Get(ctx, table)
			if err != nil {
				return err
			}
			stagers = append(stagers, stager)

			applier, err := r.appliers.Get(ctx, table)
			if err != nil {
				return err
			}
			appliers = append(appliers, applier)
		}

		// Dequeue and apply the mutations.
		mutCount := 0
		for i := range stagers {
			muts, err := stagers[i].Drain(ctx, tx, prev, resolved)
			if err != nil {
				return err
			}
			mutCount += len(muts)

			if err := appliers[i].Apply(ctx, tx, muts); err != nil {
				return err
			}
		}
		detail = types.FlushDetail(mutCount)

		return errors.WithStack(tx.Commit(ctx))
	})

	return resolved, detail, err
}

// The FOR UPDATE NOWAIT will quickly fail the query with a 55P03 code
// if another transaction is processing that timestamp.
const dequeueTemplate = `
WITH work AS (
  SELECT (source_nanos, source_logical) FROM %[1]s
  WHERE target_db=$1
    AND target_schema=$2
    AND target_applied_at IS NULL
  ORDER BY source_nanos, source_logical
  FOR UPDATE NOWAIT
  LIMIT 1)
UPDATE %[1]s
SET target_applied_at=now()
WHERE target_db=$1
  AND target_schema=$2
  AND (source_nanos, source_logical) = (SELECT * FROM work)
RETURNING source_nanos, source_logical
`

// These error values are returned by dequeueInTx to indicate why
// no timestamp was returned.
var (
	errNoWork  = errors.New("no work")
	errBlocked = errors.New("blocked")
)

// dequeueInTx locates the next unresolved timestamp to flush. It is
// extracted into its own method to make testing easier.
func (r *resolve) dequeueInTx(ctx context.Context, tx pgxtype.Querier) (hlc.Time, error) {
	var sourceNanos int64
	var sourceLogical int
	err := tx.QueryRow(ctx,
		r.sql.dequeue,
		r.target.Database().Raw(),
		r.target.Schema().Raw(),
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
