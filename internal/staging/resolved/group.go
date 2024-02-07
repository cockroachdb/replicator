// Copyright 2024 The Cockroach Authors
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

package resolved

import (
	"context"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// The secondary index allows us to find the last-known resolved
// timestamp for a target schema without running into locks held by a
// dequeued timestamp. This revscan is necessary to ensure ordering
// invariants when marking new timestamps.
const schema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_schema     STRING  NOT NULL,
  source_nanos      INT     NOT NULL,
  source_logical    INT     NOT NULL,
  target_applied_at TIMESTAMPTZ,
  source_wall_time  TIMESTAMPTZ AS (to_timestamp(source_nanos::FLOAT8 / 1e9)) VIRTUAL,
  PRIMARY KEY (target_schema, source_nanos, source_logical),
  INDEX (target_schema, source_nanos DESC, source_logical DESC)
)`

// Used for v21.X and v22.1 that don't support to_timestamp().
const schemaNoTimestamp = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_schema     STRING  NOT NULL,
  source_nanos      INT     NOT NULL,
  source_logical    INT     NOT NULL,
  target_applied_at TIMESTAMPTZ,
  PRIMARY KEY (target_schema, source_nanos, source_logical),
  INDEX (target_schema, source_nanos DESC, source_logical DESC)
)`

// Group provides durable storage of the resolved timestamps associated
// with a [types.TableGroup]. Timestamps to be processed are recorded
// with [Group.Mark] and their completion is recorded via
// [Group.Record]. This Group type will collaborate with other
// components by driving a [notify.Var] containing an [hlc.Range].
type Group struct {
	bounds     *notify.Var[hlc.Range]
	pool       *types.StagingPool
	target     *types.TableGroup
	fastWakeup notify.Var[struct{}]

	metrics struct {
		committedAge    prometheus.Gauge
		committedTime   prometheus.Gauge
		markDuration    prometheus.Observer
		proposedAge     prometheus.Gauge
		proposedTime    prometheus.Gauge
		recordDuration  prometheus.Observer
		refreshDuration prometheus.Observer
	}

	sql struct {
		mark    string
		record  string
		refresh string
	}
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

// Mark advances the resolved timestamp associated with the Group.
// It is an error if the resolved timestamp does not advance, as this
// will indicate a violation in changefeed invariants. If successful,
// this method will asynchronously refresh the Group.
func (r *Group) Mark(ctx context.Context, ts hlc.Time) error {
	err := retry.Retry(ctx, func(ctx context.Context) error {
		_, _, err := r.bounds.Update(func(old hlc.Range) (hlc.Range, error) {
			start := time.Now()
			tag, err := r.pool.Exec(ctx,
				r.sql.mark,
				r.target.Name.Canonical().Raw(),
				ts.Nanos(),
				ts.Logical(),
			)
			if err != nil {
				return old, errors.WithStack(err)
			}
			if tag.RowsAffected() == 0 {
				return old, errors.Errorf("resolved timestamp for %s is going backwards %s; "+
					"verify changefeed cursor or remove already-applied resolved timestamp entries",
					r.target, ts)
			}
			r.metrics.markDuration.Observe(time.Since(start).Seconds())
			log.WithFields(log.Fields{
				"schema":   r.target,
				"resolved": ts,
			}).Trace("marked new resolved timestamp")
			// +1 since range end is exclusive
			return hlc.Range{old.Min(), ts.Next()}, nil
		})
		return err
	})
	if err == nil {
		r.Refresh()
	}
	return err
}

const recordTemplate = `
UPDATE %s 
SET target_applied_at = now()
WHERE target_schema = $1
AND (source_nanos, source_logical) BETWEEN ($2, $3) AND ($4, $5)
AND target_applied_at IS NULL
`

// Record updates the applied-at timestamp associated with the given
// resolved timestamps in the open range [min,max). This will
// asynchronously refresh the Group.
func (r *Group) Record(ctx context.Context, rng hlc.Range) error {
	err := retry.Retry(ctx, func(ctx context.Context) error {
		start := time.Now()
		_, err := r.pool.Exec(ctx,
			r.sql.record,
			r.target.Name.Canonical().Raw(),
			rng.Min().Nanos(),
			rng.Min().Logical(),
			rng.Max().Nanos(),
			rng.Max().Logical()-1, // -1 since range end is exclusive
		)
		if err == nil {
			r.metrics.recordDuration.Observe(time.Since(start).Seconds())
		}
		return errors.WithStack(err)
	})
	if err == nil {
		log.Tracef("recorded resolved timestamps for %s: %s", r.target, rng)
		r.Refresh()
	}
	return err
}

// Refresh the Group asynchronously. This is intended for testing.
func (r *Group) Refresh() {
	r.fastWakeup.Notify()
}

// This query finds the newest applied timestamp (case A) and the newest
// unapplied timestamp (case B). An existing timestamp is used to help
// with time-based index scans. If there are no unapplied mutations, a
// closed range will be returned.
const refreshTemplate = `
WITH
a AS (
  SELECT target_schema s, source_nanos n, source_logical l
  FROM %[1]s
  WHERE target_schema = $1 AND target_applied_at IS NOT NULL 
  ORDER BY source_nanos DESC, source_logical DESC
  LIMIT 1),
b AS (
  SELECT target_schema s, source_nanos n, source_logical l
  FROM %[1]s
  WHERE target_schema = $1 AND (source_nanos, source_logical) >= ($2, $3) AND target_applied_at IS NULL
  ORDER BY source_nanos DESC, source_logical DESC
  LIMIT 1)
SELECT coalesce(a.n, 0), coalesce(a.l, 0), coalesce(b.n, a.n), coalesce(b.l, a.l)
FROM a FULL JOIN b USING (s)
LIMIT 1
`

// refreshBounds synchronizes the in-memory bounds with the database.
func (r *Group) refreshBounds(ctx context.Context) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		_, _, err := r.bounds.Update(func(old hlc.Range) (hlc.Range, error) {
			start := time.Now()
			var next hlc.Range
			var minNanos, maxNanos int64
			var minLogical, maxLogical int
			if err := r.pool.QueryRow(ctx, r.sql.refresh,
				r.target.Name.Canonical().Raw(),
				old.Min().Nanos(),
				old.Min().Logical()-1,
			).Scan(&minNanos, &minLogical, &maxNanos, &maxLogical); err == nil {
				next = hlc.Range{
					hlc.New(minNanos, minLogical),
					hlc.New(maxNanos, maxLogical+1), // +1 since end is exclusive
				}
			} else if errors.Is(err, pgx.ErrNoRows) {
				// If there's no data for this group, do nothing.
				return old, notify.ErrNoUpdate
			} else {
				return hlc.Range{}, errors.WithStack(err)
			}
			if next == old {
				log.Tracef("group %s: resolved range unchanged: %s", r.target, old)
				return old, notify.ErrNoUpdate
			}

			log.Tracef("group %s: resolved range: %s -> %s", r.target, old, next)
			r.metrics.refreshDuration.Observe(time.Since(start).Seconds())
			return next, nil
		})
		return err
	})
}

// refreshJob starts a goroutine to periodically synchronize the
// in-memory bounds with the database.
func (r *Group) refreshJob(ctx *stopper.Context) {
	ctx.Go(func() error {
		for {
			_, fastWakeup := r.fastWakeup.Get()

			// There's an immediate load when the Group is created, so
			// it's reasonable in this case to add an initial delay.
			select {
			case <-ctx.Stopping():
				return nil
			case <-fastWakeup:
			case <-time.After(time.Second):
			}

			if err := r.refreshBounds(ctx); err != nil {
				log.WithError(err).Warnf("could not refresh resolved timestamp bounds for %s; will continue", r.target)
			}
		}
	})
}

// reportMetrics starts a goroutine that will update the enclosed
// metrics as the group's resolved bounds change.
func (r *Group) reportMetrics(ctx *stopper.Context) {
	ctx.Go(func() error {
		_, err := stopvar.DoWhenChanged(ctx, hlc.Range{}, r.bounds,
			func(ctx *stopper.Context, _, bounds hlc.Range) error {
				minTime := bounds.Min().Nanos()
				maxTime := bounds.Max().Nanos()

				r.metrics.committedAge.Set(
					time.Since(time.Unix(0, minTime)).Seconds())
				r.metrics.committedTime.Set(float64(minTime) / 1e9)

				r.metrics.proposedAge.Set(
					time.Since(time.Unix(0, maxTime)).Seconds())
				r.metrics.proposedTime.Set(float64(maxTime) / 1e9)

				return nil
			})
		return err
	})
}
