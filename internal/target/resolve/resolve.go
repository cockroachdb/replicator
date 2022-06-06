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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type resolve struct {
	appliers   types.Appliers
	leases     types.Leases
	pool       *pgxpool.Pool
	stagers    types.Stagers
	target     ident.Schema
	timekeeper types.TimeKeeper
	watcher    types.Watcher

	metrics struct {
		active       prometheus.Gauge
		errors       prometheus.Counter
		lastAttempt  prometheus.Gauge
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
  target_db      STRING  NOT NULL,
  target_schema  STRING  NOT NULL,
  source_nanos   INT     NOT NULL,
  source_logical INT     NOT NULL,
  target_nanos   INT,
  target_logical INT,
  PRIMARY KEY (target_db, target_schema, source_nanos, source_logical),
  INDEX %[1]s_unresolved (target_db, target_schema, source_nanos, source_logical)
    WHERE target_nanos IS NULL AND target_logical IS NULL
)`

func newResolve(
	ctx context.Context,
	appliers types.Appliers,
	leases types.Leases,
	metaTable ident.Table,
	pool *pgxpool.Pool,
	stagers types.Stagers,
	target ident.Schema,
	timekeeper types.TimeKeeper,
	watchers types.Watchers,
) (_ *resolve, cancel func(), _ error) {
	if _, err := pool.Exec(ctx, fmt.Sprintf(schema, metaTable)); err != nil {
		return nil, func() {}, errors.WithStack(err)
	}

	watcher, err := watchers.Get(ctx, target.Database())
	if err != nil {
		return nil, func() {}, err
	}

	ret := &resolve{
		appliers:   appliers,
		leases:     leases,
		pool:       pool,
		stagers:    stagers,
		target:     target,
		timekeeper: timekeeper,
		watcher:    watcher,
	}

	labelValues := metrics.SchemaValues(target)
	ret.metrics.active = resolveActive.WithLabelValues(labelValues...)
	ret.metrics.errors = resolveFlushErrors.WithLabelValues(labelValues...)
	ret.metrics.lastAttempt = resolveLastAttempt.WithLabelValues(labelValues...)
	ret.metrics.lastSuccess = resolveLastSuccess.WithLabelValues(labelValues...)
	ret.metrics.lastResolved = resolveLastHLC.WithLabelValues(labelValues...)
	ret.metrics.successes = resolveFlushSuccess.WithLabelValues(labelValues...)

	ret.sql.dequeue = fmt.Sprintf(dequeueTemplate, metaTable)
	ret.sql.mark = fmt.Sprintf(markTemplate, metaTable)

	flushCtx, cancel := context.WithCancel(ctx)
	go ret.loop(flushCtx)

	return ret, cancel, nil
}

const markTemplate = `
INSERT INTO %[1]s (target_db, target_schema, source_nanos, source_logical)
VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING`

func (r *resolve) Mark(ctx context.Context, tx pgxtype.Querier, next hlc.Time) (bool, error) {
	tag, err := tx.Exec(ctx,
		r.sql.mark,
		r.target.Database().Raw(),
		r.target.Schema().Raw(),
		next.Nanos(),
		next.Logical(),
	)
	return tag.RowsAffected() > 0, errors.WithStack(err)
}

// loop starts a goroutine to asynchronously process resolved timestamps.
func (r *resolve) loop(ctx context.Context) {
	r.leases.Singleton(ctx, "resolve_"+r.target.Raw(), func(ctx context.Context) error {
		entry := log.WithField("schema", r.target)
		entry.Debug("resolving")
		defer entry.Debug("resolving stopped")
		r.metrics.active.Set(1)
		defer r.metrics.active.Set(0)

		for {
			r.metrics.lastAttempt.SetToCurrentTime()

			resolved, didWork, err := r.flush(ctx)
			if err != nil {
				r.metrics.errors.Inc()
				entry.WithError(err).Warn("could not resolve; will retry")
				return err
			}
			r.metrics.lastResolved.Set(float64(resolved.Nanos()))
			r.metrics.lastSuccess.SetToCurrentTime()
			r.metrics.successes.Inc()

			if didWork {
				entry.Trace("successfully resolved mutations")
				continue
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	})
}

const dequeueTemplate = `
UPDATE %[1]s
SET
  target_nanos = split_part(cluster_logical_timestamp()::STRING, '.', 1)::INT,
  target_logical = split_part(cluster_logical_timestamp()::STRING, '.', 2)::INT
WHERE target_db=$1
  AND target_schema=$2
  AND target_nanos IS NULL
  AND target_logical IS NULL
ORDER BY source_nanos, source_logical
LIMIT 1
RETURNING source_nanos, source_logical
`

// flush executes a single iteration of the asynchronous resolver logic.
// This method returns true if work was actually performed.
func (r *resolve) flush(ctx context.Context) (hlc.Time, bool, error) {
	var resolved hlc.Time
	var didWork bool

	err := retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer tx.Rollback(context.Background())

		// Load the next unresolved timestamp.
		var sourceNanos int64
		var sourceLogical int
		err = tx.QueryRow(ctx,
			r.sql.dequeue,
			r.target.Database().Raw(),
			r.target.Schema().Raw(),
		).Scan(&sourceNanos, &sourceLogical)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				log.WithField("schema", r.target).Trace("no work")
				return nil
			}
			return errors.WithStack(err)
		}
		resolved = hlc.New(sourceNanos, sourceLogical)

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
		targetTables := r.watcher.Snapshot(r.target)

		// Set up the per-table helpers.
		appliers := make([]types.Applier, 0, len(targetTables))
		stagers := make([]types.Stager, 0, len(targetTables))
		for table := range targetTables {
			stager, err := r.stagers.Get(ctx, table)
			if err != nil {
				return err
			}
			stagers = append(stagers, stager)

			// TODO(bob): Support data-driven configuration.
			applier, err := r.appliers.Get(ctx, table, nil, types.Deadlines{})
			if err != nil {
				return err
			}
			appliers = append(appliers, applier)
		}

		// Dequeue and apply the mutations.
		for i := range stagers {
			muts, err := stagers[i].Drain(ctx, tx, prev, resolved)
			if err != nil {
				return err
			}

			if err := appliers[i].Apply(ctx, tx, muts); err != nil {
				return err
			}
		}

		return errors.WithStack(tx.Commit(ctx))
	})

	return resolved, didWork, err
}
