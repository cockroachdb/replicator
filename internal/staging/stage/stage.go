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

// Package stage defines a means of storing and retrieving mutations
// to be applied to a table.
package stage

// The code in this file is reworked from sink_table.go.

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/msort"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// stagingTable returns the staging table name that will store mutations
// for the given target table.
func stagingTable(stagingDB ident.Schema, target ident.Table) ident.Table {
	target = target.Canonical() // Use lower-cased version of the table.
	mangled := ident.Join(target, ident.Raw, '_')
	return ident.NewTable(stagingDB, ident.New(mangled))
}

// stage implements a storage and retrieval mechanism for staging
// Mutation instances.
type stage struct {
	// The staging table that holds the mutations.
	stage      ident.Table
	retireFrom notify.Var[hlc.Time] // Makes subsequent calls to Retire() a bit faster.

	retireDuration prometheus.Observer
	retireError    prometheus.Counter
	selectCount    prometheus.Counter
	selectDuration prometheus.Observer
	selectError    prometheus.Counter
	staleCount     prometheus.Gauge
	storeCount     prometheus.Counter
	storeDuration  prometheus.Observer
	storeError     prometheus.Counter

	// Compute SQL fragments exactly once on startup.
	sql struct {
		retire    string // Delete a batch of staged mutations.
		store     string // Store mutations.
		unapplied string // Count stale, unapplied mutations.
	}
}

var _ types.Stager = (*stage)(nil)

const tableSchema = `
CREATE TABLE IF NOT EXISTS %[1]s (
    nanos INT NOT NULL,
  logical INT NOT NULL,
      key STRING NOT NULL,
      mut BYTES NOT NULL,
   before BYTES NULL,
  applied BOOL NOT NULL DEFAULT false,
  %[2]s
  PRIMARY KEY (nanos, logical, key),
   FAMILY cold (mut, before),
   FAMILY hot (applied)
)`

// newStore constructs a new mutation stage that will track pending
// mutations to be applied to the given target table.
func newStore(
	ctx *stopper.Context, db *types.StagingPool, stagingDB ident.Schema, target ident.Table,
) (*stage, error) {
	table := stagingTable(stagingDB, target)

	// Try to create the staging table with a helper virtual column. We
	// never query for it, so it should have essentially no cost.
	if err := retry.Execute(ctx, db, fmt.Sprintf(tableSchema, table,
		`source_time TIMESTAMPTZ AS (to_timestamp(nanos::float/1e9)) VIRTUAL,`)); err != nil {

		// Old versions of CRDB don't know about to_timestamp(). Try
		// again without the helper column.
		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
			if pgErr.Code == "42883" /* unknown function */ {
				err = retry.Execute(ctx, db, fmt.Sprintf(tableSchema, table, ""))
			}
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	// Transparently upgrade older staging tables. This avoids needing
	// to add a breaking change to the Versions slice.
	if err := retry.Execute(ctx, db, fmt.Sprintf(`
ALTER TABLE %[1]s ADD COLUMN IF NOT EXISTS before BYTES NULL
`, table)); err != nil {
		return nil, errors.WithStack(err)
	}

	labels := metrics.TableValues(target)
	s := &stage{
		stage:          table,
		retireDuration: stageRetireDurations.WithLabelValues(labels...),
		retireError:    stageRetireErrors.WithLabelValues(labels...),
		selectCount:    stageSelectCount.WithLabelValues(labels...),
		selectDuration: stageSelectDurations.WithLabelValues(labels...),
		selectError:    stageSelectErrors.WithLabelValues(labels...),
		staleCount:     stageStaleMutations.WithLabelValues(labels...),
		storeCount:     stageStoreCount.WithLabelValues(labels...),
		storeDuration:  stageStoreDurations.WithLabelValues(labels...),
		storeError:     stageStoreErrors.WithLabelValues(labels...),
	}

	s.sql.retire = fmt.Sprintf(retireTemplate, table)
	s.sql.store = fmt.Sprintf(putTemplate, table)
	s.sql.unapplied = fmt.Sprintf(countTemplate, table)

	// Report unapplied mutations on a periodic basis.
	ctx.Go(func() error {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			// We don't want to select on the notification channel,
			// since this value may be updated at a high rate on the
			// instance of cdc-sink that holds the resolver lease.
			from, _ := s.retireFrom.Get()
			ct, err := s.CountUnapplied(ctx, db, from)
			if err != nil {
				log.WithError(err).Warnf("could not count unapplied mutations for target: %s", target)
			} else {
				s.staleCount.Set(float64(ct))
			}

			select {
			case <-ctx.Stopping():
				return nil
			case <-ticker.C:
				// Ensure that values get reset if this instance of
				// cdc-sink isn't the one that's actively resolving or
				// retiring mutations.
			}
		}
	})

	return s, nil
}

const countTemplate = `
SELECT count(*) FROM %s
WHERE (nanos, logical) < ($1, $2) AND NOT applied
`

// CountUnapplied returns the number of dangling mutations that likely
// indicate an error condition.
func (s *stage) CountUnapplied(
	ctx context.Context, db types.StagingQuerier, before hlc.Time,
) (int, error) {
	var ret int
	err := retry.Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(ctx, s.sql.unapplied, before.Nanos(), before.Logical()).Scan(&ret)
	})
	return ret, errors.Wrap(err, s.sql.unapplied)
}

// GetTable returns the table that the stage is storing into.
func (s *stage) GetTable() ident.Table { return s.stage }

// The byte-array casts on $4 and $5 are because arrays of JSONB aren't implemented:
// https://github.com/cockroachdb/cockroach/issues/23468
const putTemplate = `
UPSERT INTO %s (nanos, logical, key, mut, before)
SELECT unnest($1::INT[]), unnest($2::INT[]), unnest($3::STRING[]), unnest($4::BYTES[]), unnest($5::BYTES[])`

// Store stores some number of Mutations into the database.
func (s *stage) Store(
	ctx context.Context, db types.StagingQuerier, mutations []types.Mutation,
) error {
	start := time.Now()

	mutations = msort.UniqueByTimeKey(mutations)

	// If we're working with a pool, and not a transaction, we'll stage
	// the data in a concurrent manner.
	var err error
	if _, isPool := db.(*types.StagingPool); isPool {
		eg, errCtx := errgroup.WithContext(ctx)
		err = batches.Batch(len(mutations), func(begin, end int) error {
			eg.Go(func() error {
				return s.putOne(errCtx, db, mutations[begin:end])
			})
			return nil
		})
		if err != nil {
			return err
		}
		err = eg.Wait()
	} else {
		err = batches.Batch(len(mutations), func(begin, end int) error {
			return s.putOne(ctx, db, mutations[begin:end])
		})
	}

	if err != nil {
		s.storeError.Inc()
		return err
	}

	d := time.Since(start)
	s.storeCount.Add(float64(len(mutations)))
	s.storeDuration.Observe(d.Seconds())
	log.WithFields(log.Fields{
		"count":    len(mutations),
		"duration": d,
		"target":   s.stage,
	}).Debug("stored mutations")
	return nil
}

func (s *stage) putOne(
	ctx context.Context, db types.StagingQuerier, mutations []types.Mutation,
) error {
	nanos := make([]int64, len(mutations))
	logical := make([]int, len(mutations))
	keys := make([]string, len(mutations))
	jsons := make([][]byte, len(mutations))
	befores := make([][]byte, len(mutations))

	numWorkers := runtime.GOMAXPROCS(0)
	eg, errCtx := errgroup.WithContext(ctx)
	for worker := 0; worker < numWorkers; worker++ {
		worker := worker
		eg.Go(func() error {
			for idx := worker; idx < len(jsons); idx += numWorkers {
				if err := errCtx.Err(); err != nil {
					return err
				}
				var err error
				mut := mutations[idx]

				nanos[idx] = mut.Time.Nanos()
				logical[idx] = mut.Time.Logical()
				keys[idx] = string(mut.Key)
				befores[idx], err = maybeGZip(mut.Before)
				if err != nil {
					return err
				}

				if mut.IsDelete() {
					jsons[idx] = []byte("null")
					continue
				}

				jsons[idx], err = maybeGZip(mut.Data)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	_, err := db.Exec(ctx, s.sql.store, nanos, logical, keys, jsons, befores)
	return errors.Wrap(err, s.sql.store)
}

const retireTemplate = `
WITH d AS (
     DELETE FROM %s
      WHERE (nanos, logical) BETWEEN ($1, $2) AND ($3, $4) AND applied
   ORDER BY nanos, logical
      LIMIT $5
  RETURNING nanos, logical)
SELECT last_value(nanos) OVER (), last_value(logical) OVER ()
  FROM d
 LIMIT 1`

// Retire deletes staged data up to the given end time.
func (s *stage) Retire(ctx context.Context, db types.StagingQuerier, end hlc.Time) error {
	start := time.Now()
	err := retry.Retry(ctx, func(ctx context.Context) error {
		from, _ := s.retireFrom.Get()
		for hlc.Compare(from, end) < 0 {
			var lastNanos int64
			var lastLogical int
			err := db.QueryRow(ctx, s.sql.retire,
				from.Nanos(),
				from.Logical(),
				end.Nanos(),
				end.Logical(),
				10000, // Make configurable?
			).Scan(&lastNanos, &lastLogical)

			if errors.Is(err, pgx.ErrNoRows) {
				break
			}
			if err != nil {
				return errors.WithStack(err)
			}
			from = hlc.New(lastNanos, lastLogical)
		}
		// If there was nothing to delete, still advance the marker.
		if hlc.Compare(from, end) < 0 {
			from = end
		}
		s.retireFrom.Set(from)
		return nil
	})
	if err == nil {
		s.retireDuration.Observe(time.Since(start).Seconds())
	} else {
		s.retireError.Inc()
	}
	return err
}
