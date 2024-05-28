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
	"database/sql"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// The secondary index allows us to find the last-known checkpoint
// timestamp for a target schema.
const schema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  group_name        STRING      NOT NULL,
  source_hlc        DECIMAL     NOT NULL,
  partition         STRING      NOT NULL,
  first_seen        TIMESTAMPTZ NOT NULL DEFAULT now(),
  target_applied_at TIMESTAMPTZ,

-- Virtual columns for DBA convenience
  source_nanos      INT8        AS (floor(source_hlc)::INT8) VIRTUAL,
  source_logical    INT8        AS (((source_hlc-floor(source_hlc))*1e10)::INT8) VIRTUAL,
  source_wall_time  TIMESTAMPTZ AS (to_timestamp(floor(source_hlc)::FLOAT8 / 1e9)) VIRTUAL,
  PRIMARY KEY (group_name, source_hlc, partition),
  INDEX (group_name, target_applied_at, partition)
)`

// Group provides durable storage of the checkpoint (FKA resolved)
// timestamps associated with a [types.TableGroup].
//
// Timestamps to be processed are recorded with [Group.Advance] and
// their completion is recorded via [Group.Commit]. A checkpoint may
// consist of an arbitrary number of partitions, which may be
// pre-created with [Group.Ensure].
//
//	This Group type will collaborate with other components by driving a
//	[notify.Var] containing an [hlc.Range]. This range represents a
//	window of eligible checkpoints that require processing, where the
//	minimum is called "committed" and the maximum is called "proposed".
//	The proposed time is the least common maximum value across all
//	partitions of a checkpoint, whereas the commit time is always common
//	to all partitions.
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
		advance string
		commit  string
		ensure  string
		refresh string
	}
}

// This query conditionally inserts a new mark for a partition if
// there is no previous mark or if the proposed mark is equal to or
// after the latest-known mark for the target partition.
//
// $1 = group_name
// $2 = partition
// $3 = source_hlc
const advanceTemplate = `
WITH
not_before AS (
  SELECT source_hlc FROM %[1]s
  WHERE group_name=$1 AND partition=$2 
  ORDER BY source_hlc DESC
  FOR UPDATE
  LIMIT 1),
to_insert AS (
  SELECT $1::STRING, $2::STRING, $3::DECIMAL
  WHERE (SELECT count(*) FROM not_before) = 0
     OR ($3::DECIMAL) >= (SELECT source_hlc FROM not_before))
UPSERT INTO %[1]s (group_name, partition, source_hlc)
SELECT * FROM to_insert`

// Advance extends the proposed checkpoint timestamp associated with the
// partition of the Group. It is an error if the timestamp does not
// advance beyond its current point, as this will indicate a violation
// of changefeed invariants. If successful, this method will
// asynchronously refresh the Group.
func (r *Group) Advance(ctx context.Context, partition ident.Ident, ts hlc.Time) error {
	start := time.Now()
	tag, err := r.pool.Exec(ctx,
		r.sql.advance,
		r.target.Name.Canonical().Raw(),
		partition.Canonical().Raw(),
		ts,
	)
	if err != nil {
		return errors.WithStack(err)
	}
	if tag.RowsAffected() == 0 {
		r.metrics.backwards.Inc()
		return errors.Errorf(
			"proposed checkpoint timestamp for group=%s, partition=%s is going backwards: %s; "+
				"verify changefeed cursor or remove already-applied "+
				"checkpoint timestamp entries",
			r.target, partition, ts)
	}
	r.Refresh()

	r.metrics.advanceDuration.Observe(time.Since(start).Seconds())
	log.WithFields(log.Fields{
		"checkpoint": ts,
		"group":      r.target,
		"partition":  partition,
	}).Trace("advanced checkpoint timestamp")
	return nil
}

// Sets target_applied_at to now().
//
// $1 = group_name
// $2 = hlc_min
// $3 = hlc_max
const commitTemplate = `
UPDATE %s 
SET target_applied_at = now()
WHERE group_name = $1
AND source_hlc >= $2 AND source_hlc < $3
AND target_applied_at IS NULL
`

// Commit updates the applied-at timestamp associated with the
// checkpoints in the open range [min,max). This will asynchronously
// refresh the Group.
func (r *Group) Commit(ctx context.Context, rng hlc.Range) error {
	err := retry.Retry(ctx, r.pool, func(ctx context.Context) error {
		start := time.Now()
		_, err := r.pool.Exec(ctx,
			r.sql.commit,
			r.target.Name.Canonical().Raw(),
			rng.Min(),
			rng.Max(),
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

const ensureTemplate = `
UPSERT INTO %[1]s (group_name, partition, source_hlc, target_applied_at)
SELECT $1, $2, 1.0000000001, now()
 WHERE NOT EXISTS (SELECT 1 FROM %[1]s WHERE group_name=$1 AND partition=$2)
`

// Ensure that a checkpoint exists for all named partitions. If no
// checkpoint exists for a given partition, an applied, minimum-valued
// checkpoint will be created. This method can be used to expand the
// number of partitions associated with a group at any point in time.
func (r *Group) Ensure(ctx context.Context, partitions []ident.Ident) error {
	err := retry.Retry(ctx, r.pool, func(ctx context.Context) error {
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		for _, part := range partitions {
			if _, err := tx.Exec(ctx, r.sql.ensure, r.target.Name.Raw(), part.Raw()); err != nil {
				return errors.Wrap(err, r.sql.ensure)
			}
		}
		return errors.WithStack(tx.Commit(ctx))
	})
	if err == nil {
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

// This query computes the open checkpoint window for the entire group.
// That is, it returns the newest applied and newest unapplied
// checkpoint times across all partitions of the group.
//
// Params:
//   - $1: group name
//   - $2: last successful checkpoint to reduce table scan range
//
// CTE components:
//   - available_data: A buffer of data after the previous checkpoint.
//   - partition_max_times: Determines the latest known checkpoint for
//     each partition within the group.
//   - visible_data: Restricts available_data by the minimum-maximum
//     value from p_max_times.
//   - partition_max_unapplied: Finds the last checkpoint time that
//     hasn't been processed.
//   - last_applied: Finds the latest applied timestamp within
//     visible_data. This is almost always going to be the start point.
//   - stop: Finds the minimum unapplied timestamp within the visible
//     data. If there are no unapplied timestamps, this will return the
//     last-applied time to create an empty range.
//   - look_back: Handles a degenerate case where there's a gap in the
//     checkpoint entries and last_applied is actually after the stop
//     point.
const refreshTemplate = `
WITH
available_data AS (
SELECT partition, source_hlc, target_applied_at
  FROM %[1]s
 WHERE group_name = $1
   AND source_hlc >= $2
),
partition_max_times AS (
SELECT partition, max(source_hlc) AS hlc
  FROM available_data
 GROUP BY partition
),
visible_data AS (
SELECT *
  FROM available_data
 WHERE source_hlc <= (SELECT min(hlc) FROM partition_max_times)
),
partition_max_unapplied AS (
SELECT partition, max(source_hlc) AS hlc
  FROM visible_data
 WHERE target_applied_at IS NULL
 GROUP BY partition
),
last_applied AS (
SELECT max(source_hlc) AS hlc
  FROM visible_data
 WHERE target_applied_at IS NOT NULL
),
stop AS (
SELECT min(COALESCE(partition_max_unapplied.hlc, (SELECT hlc FROM last_applied))) AS hlc
  FROM (SELECT DISTINCT partition FROM visible_data)
  LEFT JOIN partition_max_unapplied USING (partition)
),
look_back AS (
SELECT max(source_hlc) AS hlc
  FROM visible_data
 WHERE target_applied_at IS NOT NULL
   AND source_hlc <= (SELECT hlc FROM stop)
)
SELECT
  (SELECT hlc FROM look_back),
  (SELECT hlc FROM stop)
`

// refreshBounds synchronizes the in-memory bounds with the database.
func (r *Group) refreshBounds(ctx context.Context) error {
	return retry.Retry(ctx, r.pool, func(ctx context.Context) error {
		_, _, err := r.bounds.Update(func(old hlc.Range) (hlc.Range, error) {
			start := time.Now()
			next, err := r.refreshQuery(ctx, old.Min())
			if err != nil {
				return hlc.RangeEmpty(), nil
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

func (r *Group) refreshQuery(ctx context.Context, knownCommitted hlc.Time) (hlc.Range, error) {
	var ret hlc.Range
	err := retry.Retry(ctx, r.pool, func(ctx context.Context) error {
		var nextMin, nextMax sql.Null[hlc.Time]
		if err := r.pool.QueryRow(ctx, r.sql.refresh,
			r.target.Name.Canonical().Raw(),
			knownCommitted,
		).Scan(&nextMin, &nextMax); err != nil {
			return errors.WithStack(err)
		}

		if !nextMin.Valid {
			nextMin.V = knownCommitted
		}

		if nextMax.Valid {
			ret = hlc.RangeIncluding(nextMin.V, nextMax.V)
		} else {
			ret = hlc.RangeIncluding(nextMin.V, nextMin.V)
		}

		return nil
	})
	return ret, err
}

// refreshJob starts a goroutine to periodically synchronize the
// in-memory bounds with the database.
func (r *Group) refreshJob(ctx *stopper.Context) {
	ctx.Go(func(ctx *stopper.Context) error {
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
	ctx.Go(func(ctx *stopper.Context) error {
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
