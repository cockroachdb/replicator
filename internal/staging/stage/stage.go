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
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// stagingTable returns the staging table name that will store mutations
// for the given target table.
func stagingTable(stagingDB ident.Ident, target ident.Table) ident.Table {
	tblName := strings.Join(
		[]string{target.Database().Raw(), target.Schema().Raw(), target.Table().Raw()}, "_")
	return ident.NewTable(stagingDB, ident.Public, ident.New(tblName))
}

// stage implements a storage and retrieval mechanism for staging
// Mutation instances.
type stage struct {
	// The staging table that holds the mutations.
	stage      ident.Table
	retireFrom hlc.Time // Makes subsequent calls to Retire() a bit faster.

	retireDuration prometheus.Observer
	retireError    prometheus.Counter
	selectCount    prometheus.Counter
	selectDuration prometheus.Observer
	selectError    prometheus.Counter
	storeCount     prometheus.Counter
	storeDuration  prometheus.Observer
	storeError     prometheus.Counter

	// Compute SQL fragments exactly once on startup.
	sql struct {
		nextAfter  string // Find a timestamp for which data is available.
		retire     string // Delete a batch of staged mutations
		store      string // store mutations
		sel        string // select all rows in the timeframe from the staging table
		selPartial string // select limited number of rows from the staging table
	}
}

var _ types.Stager = (*stage)(nil)

// newStore constructs a new mutation stage that will track pending
// mutations to be applied to the given target table.
func newStore(
	ctx context.Context, db *types.StagingPool, stagingDB ident.Ident, target ident.Table,
) (*stage, error) {
	table := stagingTable(stagingDB, target)

	if err := retry.Execute(ctx, db, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	nanos INT NOT NULL,
  logical INT NOT NULL,
	  key STRING NOT NULL,
	  mut BYTES NOT NULL,
	PRIMARY KEY (nanos, logical, key)
)`, table)); err != nil {
		return nil, err
	}

	labels := metrics.TableValues(target)
	s := &stage{
		stage:          table,
		retireDuration: stageRetireDurations.WithLabelValues(labels...),
		retireError:    stageRetireErrors.WithLabelValues(labels...),
		selectCount:    stageSelectCount.WithLabelValues(labels...),
		selectDuration: stageSelectDurations.WithLabelValues(labels...),
		selectError:    stageSelectErrors.WithLabelValues(labels...),
		storeCount:     stageStoreCount.WithLabelValues(labels...),
		storeDuration:  stageStoreDurations.WithLabelValues(labels...),
		storeError:     stageStoreErrors.WithLabelValues(labels...),
	}

	s.sql.nextAfter = fmt.Sprintf(nextAfterTemplate, table)
	s.sql.retire = fmt.Sprintf(retireTemplate, table)
	s.sql.sel = fmt.Sprintf(selectTemplateAll, table)
	s.sql.selPartial = fmt.Sprintf(selectTemplatePartial, table)
	s.sql.store = fmt.Sprintf(putTemplate, table)

	return s, nil
}

// GetTable returns the table that the stage is storing into.
func (s *stage) GetTable() ident.Table { return s.stage }

const nextAfterTemplate = `
SELECT DISTINCT nanos, logical
 FROM %[1]s
WHERE (nanos, logical) > ($1, $2) AND (nanos, logical) <= ($3, $4)
ORDER BY nanos, logical
`

// TransactionTimes implements types.Stager and returns timestamps for
// which data is available in the (before, after] range.
func (s *stage) TransactionTimes(
	ctx context.Context, tx types.StagingQuerier, before, after hlc.Time,
) ([]hlc.Time, error) {
	var ret []hlc.Time
	err := retry.Retry(ctx, func(ctx context.Context) error {
		ret = ret[:0] // Reset if retrying.
		rows, err := tx.Query(ctx,
			s.sql.nextAfter,
			before.Nanos(),
			before.Logical(),
			after.Nanos(),
			after.Logical(),
		)
		if err != nil {
			return errors.WithStack(err)
		}

		for rows.Next() {
			var nanos int64
			var logical int
			if err := rows.Scan(&nanos, &logical); err != nil {
				return errors.WithStack(err)
			}
			ret = append(ret, hlc.New(nanos, logical))
		}
		return errors.WithStack(rows.Err())
	})

	return ret, err
}

// ($1, $2) starting resolved timestamp
// ($3, $4) ending resolved timestamp
//
// This query drains all mutations between the given timestamps,
// returning the latest timestamped value for any given key.
const selectTemplateAll = `
SELECT key, nanos, logical, mut
  FROM %[1]s
 WHERE (nanos, logical) BETWEEN ($1, $2) AND ($3, $4)
 ORDER BY nanos, logical`

// ($1, $2) starting resolved timestamp
// ($3, $4) ending resolved timestamp
// $5 starting key to skip
// $6 limit
//
// For the kind of very large datasets that we see in a backfill
// scenario, it's not feasible to deduplicate updates for a given key
// with in the time range.  We would have to scan forward to all
// timestamps within the given timestamp range to see if there's a key
// value to be had. An index over (key, nanos, time) doesn't help,
// either for key-based pagination, since we now have to read through
// all keys to identify those with a mutation in the desired window.
const selectTemplatePartial = `
SELECT key, nanos, logical, mut
  FROM %[1]s
 WHERE (nanos, logical, key) > ($1, $2, $5)
   AND (nanos, logical) <= ($3, $4) 
 ORDER BY nanos, logical, key
 LIMIT $6`

// Select implements types.Stager.
func (s *stage) Select(
	ctx context.Context, tx types.StagingQuerier, prev, next hlc.Time,
) ([]types.Mutation, error) {
	return s.SelectPartial(ctx, tx, prev, next, nil, -1)
}

// SelectPartial implements types.Stager.
func (s *stage) SelectPartial(
	ctx context.Context, tx types.StagingQuerier, prev, next hlc.Time, afterKey []byte, limit int,
) ([]types.Mutation, error) {
	if hlc.Compare(prev, next) > 0 {
		return nil, errors.Errorf("timestamps out of order: %s > %s", prev, next)
	}

	start := time.Now()
	var ret []types.Mutation
	if limit > 0 {
		ret = make([]types.Mutation, 0, limit)
	}

	err := retry.Retry(ctx, func(ctx context.Context) error {
		ret = ret[:0]
		var rows pgx.Rows
		var err error
		if limit <= 0 {
			rows, err = tx.Query(ctx, s.sql.sel,
				prev.Nanos(), prev.Logical(), next.Nanos(), next.Logical())
		} else {
			rows, err = tx.Query(ctx, s.sql.selPartial,
				prev.Nanos(), prev.Logical(), next.Nanos(), next.Logical(), string(afterKey), limit)
		}
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var mut types.Mutation
			var nanos int64
			var logical int
			if err := rows.Scan(&mut.Key, &nanos, &logical, &mut.Data); err != nil {
				return err
			}
			// Check for gzip magic numbers.
			if len(mut.Data) >= 2 && mut.Data[0] == 0x1f && mut.Data[1] == 0x8b {
				r, err := gzip.NewReader(bytes.NewReader(mut.Data))
				if err != nil {
					return errors.WithStack(err)
				}
				mut.Data, err = io.ReadAll(r)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			mut.Time = hlc.New(nanos, logical)
			ret = append(ret, mut)
		}
		return nil
	})

	if err != nil {
		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
			// Staging table not found. Most likely cause is a reference
			// table which was restored via backup for which we haven't
			// seen any updates.
			if pgErr.Code == "42P01" {
				return nil, nil
			}
		}
		s.selectError.Inc()
		return nil, errors.Wrapf(err, "select %s [%s, %s]", s.stage, prev, next)
	}

	// Don't bother recording stats about no-op selects.
	if len(ret) == 0 {
		return nil, nil
	}

	d := time.Since(start)
	s.selectDuration.Observe(d.Seconds())
	s.selectCount.Add(float64(len(ret)))
	log.WithFields(log.Fields{
		"count":    len(ret),
		"duration": d,
		"next":     next,
		"prev":     prev,
		"target":   s.stage,
	}).Debug("select mutations")
	return ret, nil
}

// The extra cast on $4 is because arrays of JSONB aren't implemented:
// https://github.com/cockroachdb/cockroach/issues/23468
const putTemplate = `
UPSERT INTO %s (nanos, logical, key, mut)
SELECT unnest($1::INT[]), unnest($2::INT[]), unnest($3::STRING[]), unnest($4::BYTES[])`

// Store stores some number of Mutations into the database.
func (s *stage) Store(
	ctx context.Context, db types.StagingQuerier, mutations []types.Mutation,
) error {
	start := time.Now()

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

	numWorkers := runtime.GOMAXPROCS(0)
	eg, errCtx := errgroup.WithContext(ctx)
	for worker := 0; worker < numWorkers; worker++ {
		worker := worker
		eg.Go(func() error {
			// We use w.Reset() below
			var gzWriter gzip.Writer
			for idx := worker; idx < len(jsons); idx += numWorkers {
				if err := errCtx.Err(); err != nil {
					return err
				}
				mut := mutations[idx]

				nanos[idx] = mut.Time.Nanos()
				logical[idx] = mut.Time.Logical()
				keys[idx] = string(mut.Key)

				if mut.IsDelete() {
					jsons[idx] = []byte("null")
					continue
				}

				var buf bytes.Buffer
				gzWriter.Reset(&buf)
				if _, err := gzWriter.Write(mut.Data); err != nil {
					return errors.WithStack(err)
				}
				if err := gzWriter.Close(); err != nil {
					return errors.WithStack(err)
				}
				if buf.Len() < len(mut.Data) {
					jsons[idx] = buf.Bytes()
				} else {
					jsons[idx] = mut.Data
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	_, err := db.Exec(ctx, s.sql.store, nanos, logical, keys, jsons)
	return errors.Wrap(err, s.sql.store)
}

const retireTemplate = `
WITH d AS (
     DELETE FROM %s
      WHERE (nanos, logical) BETWEEN ($1, $2) AND ($3, $4)
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
		for hlc.Compare(s.retireFrom, end) < 0 {
			var lastNanos int64
			var lastLogical int
			err := db.QueryRow(ctx, s.sql.retire,
				s.retireFrom.Nanos(),
				s.retireFrom.Logical(),
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
			s.retireFrom = hlc.New(lastNanos, lastLogical)
		}
		// If there was nothing to delete, still advance the marker.
		if hlc.Compare(s.retireFrom, end) < 0 {
			s.retireFrom = end
		}
		return nil
	})
	if err == nil {
		s.retireDuration.Observe(time.Since(start).Seconds())
	} else {
		s.retireError.Inc()
	}
	return err
}
