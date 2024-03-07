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

package checkpoint

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

// The secondary index allows us to find the last-known checkpoint
// timestamp for a target schema.
const schema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_schema     STRING      NOT NULL,
  source_nanos      INT         NOT NULL,
  source_logical    INT         NOT NULL,
  first_seen        TIMESTAMPTZ NOT NULL DEFAULT now(),
  target_applied_at TIMESTAMPTZ,
  source_wall_time  TIMESTAMPTZ AS (to_timestamp(source_nanos::FLOAT8 / 1e9)) VIRTUAL,
  PRIMARY KEY (target_schema, source_nanos, source_logical),
  INDEX (target_schema, source_nanos DESC, source_logical DESC)
)`

// Used for v21.X and v22.1 that don't support to_timestamp().
const schemaNoTimestamp = `
CREATE TABLE IF NOT EXISTS %[1]s (
  target_schema     STRING      NOT NULL,
  source_nanos      INT         NOT NULL,
  source_logical    INT         NOT NULL,
  first_seen        TIMESTAMPTZ NOT NULL DEFAULT now(),
  target_applied_at TIMESTAMPTZ,
  PRIMARY KEY (target_schema, source_nanos, source_logical),
  INDEX (target_schema, source_nanos DESC, source_logical DESC)
)`

// Group provides durable storage of the checkpoint (FKA resolved)
// timestamps associated with a [types.TableGroup]. Timestamps to be
// processed are recorded with [Group.Advance] and their completion is
// recorded via [Group.Commit]. This Group type will collaborate with
// other components by driving a [notify.Var] containing an [hlc.Range].
// This range represents a window of eligible checkpoints that require
// processing, where the minimum is called "committed" and the maximum
// is called "proposed".
type Group struct {
	bounds     *notify.Var[hlc.Range]
	pool       *types.StagingPool
	target     *types.TableGroup
	fastWakeup notify.Var[struct{}]

	metrics struct {
		advanceDuration prometheus.Observer
		backwards       prometheus.Counter
		commitDuration  prometheus.Observer
		committedAge    prometheus.Gauge
		committedTime   prometheus.Gauge
		proposedAge     prometheus.Gauge
		proposedTime    prometheus.Gauge
		refreshDuration prometheus.Observer
	}

	sql struct {
		mark    string
		record  string
		refresh string
	}
}

// This query conditionally inserts a new mark for a target schema if
// there is no previous mark or if the proposed mark is equal to or
// after the latest-known mark for the target schema.
//
// $1 = target_schema
// $2 = source_nanos
// $3 = source_logical
const advanceTemplate = `
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
     OR ($2::INT, $3::INT) >= (SELECT (source_nanos, source_logical) FROM not_before))
UPSERT INTO %[1]s (target_schema, source_nanos, source_logical)
SELECT * FROM to_insert`

// Advance extends the proposed checkpoint timestamp associated with the
// Group. It is an error if the timestamp does not advance beyond its
// current point, as this will indicate a violation of changefeed
// invariants. If successful, this method will asynchronously refresh
// the Group.
func (r *Group) Advance(ctx context.Context, ts hlc.Time) error {
	start := time.Now()
	tag, err := r.pool.Exec(ctx,
		r.sql.mark,
		r.target.Name.Canonical().Raw(),
		ts.Nanos(),
		ts.Logical(),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	if tag.RowsAffected() == 0 {
		r.metrics.backwards.Inc()
		return errors.Errorf(
			"proposed checkpoint timestamp for %s is going backwards %s; "+
				"verify changefeed cursor or remove already-applied "+
				"checkpoint timestamp entries",
			r.target, ts)
	}
	r.Refresh()

	r.metrics.advanceDuration.Observe(time.Since(start).Seconds())
	log.WithFields(log.Fields{
		"checkpoint": ts,
		"group":      r.target,
	}).Trace("advanced checkpoint timestamp")
	return nil
}

const applyTemplate = `
UPDATE %s 
SET target_applied_at = now()
WHERE target_schema = $1
AND (source_nanos, source_logical) >= ($2, $3) AND (source_nanos, source_logical) < ($4, $5)
AND target_applied_at IS NULL
`

// Commit updates the applied-at timestamp associated with the
// checkpoints in the open range [min,max). This will asynchronously
// refresh the Group.
func (r *Group) Commit(ctx context.Context, rng hlc.Range) error {
	err := retry.Retry(ctx, func(ctx context.Context) error {
		start := time.Now()
		_, err := r.pool.Exec(ctx,
			r.sql.record,
			r.target.Name.Canonical().Raw(),
			rng.Min().Nanos(),
			rng.Min().Logical(),
			rng.Max().Nanos(),
			rng.Max().Logical(),
		)
		if err == nil {
			r.metrics.commitDuration.Observe(time.Since(start).Seconds())
		}
		return errors.WithStack(err)
	})
	if err == nil {
		log.Tracef("recorded checkpoint timestamps for %s: %s", r.target, rng)
		r.Refresh()
	}
	return err
}

// Refresh the Group asynchronously. This is intended for testing.
func (r *Group) Refresh() {
	r.fastWakeup.Notify()
}

// TableGroup returns the [types.TableGroup] whose checkpoints are being
// persisted.
func (r *Group) TableGroup() *types.TableGroup {
	return r.target
}

// This query locates the newest applied timestamp and returns the next
// unapplied timestamp
//
// Params:
//   - $1: target schema
//   - $2: last successful checkpoint nanos
//   - $3: last successful checkpoint logical
//
// CTE components:
//   - pairs: Generates pairs of candidate start/end rows. The coalesce
//     functions allow us to handle the case where there is exactly one
//     unapplied row; we'll get the previous successful timestamp.
//   - ideal: Selects the first pair that has an applied start time and
//     an unapplied end time. This should be the general case when we're
//     processing data.
//   - complete: Handles the case where there are no unresolved
//     timestamps remaining. This query returns an empty range
//     beyond the final applied checkpoint.
const refreshTemplate = `
WITH
pairs AS (
SELECT
  COALESCE(lag(source_nanos) OVER (), $2) start_n,
  COALESCE(lag(source_logical) OVER (), $3) start_l,
  COALESCE(lag(target_applied_at IS NOT NULL) OVER (), true) start_applied,
  source_nanos end_n,
  source_logical end_l,
  target_applied_at IS NOT NULL end_applied
 FROM %[1]s
WHERE target_schema = $1
  AND (source_nanos, source_logical) >= ($2, $3)
ORDER BY source_nanos, source_logical
),
ideal AS (
 SELECT start_n, start_l, end_n, end_l
  FROM pairs
 WHERE start_applied AND NOT end_applied
 LIMIT 1
),
complete AS (
SELECT
  last_value(end_n) OVER w,
  last_value(end_l) OVER w,
  last_value(end_n) OVER w,
  last_value(end_l) OVER w
  FROM pairs
  WHERE NOT EXISTS (SELECT * FROM ideal) AND end_applied
  WINDOW w AS (ORDER BY end_n, end_l ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT * FROM ideal UNION ALL
SELECT * FROM complete
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
				old.Min().Logical(),
			).Scan(&minNanos, &minLogical, &maxNanos, &maxLogical); err == nil {
				next = hlc.RangeIncluding(
					hlc.New(minNanos, minLogical),
					hlc.New(maxNanos, maxLogical),
				)
			} else if errors.Is(err, pgx.ErrNoRows) {
				// If there's no data for this group, do nothing.
				return old, notify.ErrNoUpdate
			} else {
				return hlc.RangeEmpty(), errors.WithStack(err)
			}
			if next == old {
				log.Tracef("group %s: checkpoint range unchanged: %s", r.target, old)
				return old, notify.ErrNoUpdate
			}

			log.Tracef("group %s: checkpoint range: %s -> %s", r.target, old, next)
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
				log.WithError(err).Warnf("could not refresh checkpoint timestamp "+
					"bounds for %s; will continue", r.target)
			}
		}
	})
}

// reportMetrics starts a goroutine that will update the enclosed
// metrics as the group's bounds change. The refresh is also periodic to
// allow age metrics to tick.
func (r *Group) reportMetrics(ctx *stopper.Context) {
	ctx.Go(func() error {
		_, err := stopvar.DoWhenChangedOrInterval(ctx,
			hlc.RangeEmpty(), r.bounds, 5*time.Second,
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
