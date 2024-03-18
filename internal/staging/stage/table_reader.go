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

package stage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type tableCursor struct {
	Batch    *types.MultiBatch // Multiple timestamps, but only a single table. Never nil.
	Error    error             // Fatal error, must be restarted.
	Fragment bool              // Indicates the last TemporalBatch may not be complete.
	Jump     bool              // True if there's a discontinuity in the bounds.
	Progress hlc.Range         // Shows the range of data that was scanned.
}

// tableReader returns batches of rows from an individual table.
type tableReader struct {
	accumulator  []types.Mutation       // Accumulates rows as we go.
	bounds       *notify.Var[hlc.Range] // Timestamps to read within.
	db           *types.StagingPool     // Access to the staging database.
	fragmentSize int                    // Upper bound on the size of data we'll send.
	out          chan<- *tableCursor    // Communicate to the caller.
	scanBounds   hlc.Range              // The remaining range of data to scan.
	scanKey      json.RawMessage        // Position within the table.
	sqlQ         string                 // The SQL query that drives the tableReader.
	table        ident.Table            // Names of target table.

	readCount     prometheus.Counter
	readDurations prometheus.Observer
	readLag       prometheus.Gauge
	readQueue     prometheus.Gauge
}

// Basic pagination-style query.
//   - ($1, $2, $3): Start position, nanos, logical key
//   - ($4, $5): End position nanos, logical
//   - $6: Row limit
const readTableTemplate = `
SELECT nanos, logical, key, mut, before
FROM %s
WHERE (nanos, logical, key) > ($1::INT8, $2::INT8, COALESCE($3::STRING, ''))
AND (nanos, logical) < ($4::INT8, $5::INT8)
AND NOT applied
ORDER BY nanos, logical, key
LIMIT $6
`

func newTableReader(
	bounds *notify.Var[hlc.Range],
	db *types.StagingPool,
	fragmentSize int,
	out chan<- *tableCursor,
	stagingDB ident.Schema,
	target ident.Table,
) *tableReader {
	labels := metrics.TableValues(target)

	return &tableReader{
		bounds:       bounds,
		db:           db,
		fragmentSize: fragmentSize,
		sqlQ:         fmt.Sprintf(readTableTemplate, stagingTable(stagingDB, target)),
		out:          out,
		table:        target,

		readCount:     stageReadRows.WithLabelValues(labels...),
		readDurations: stageReadDurations.WithLabelValues(labels...),
		readLag:       stageReadLag.WithLabelValues(labels...),
		readQueue:     stageReadQueue.WithLabelValues(labels...),
	}
}

// run assumes it's being executed via its own goroutine. It will close
// the output channel.
func (r *tableReader) run(ctx *stopper.Context) {
	defer close(r.out)

	bounds, boundsChanged := r.bounds.Get()
	r.updateBounds(bounds)
	didJump := false

	// We're not using stopvar.DoWhenChanged since we want to refresh
	// the bounds in the middle of the loop.
	for {
		if !bounds.Empty() {
			cursor := r.nextCursor(ctx)

			// Indicate discontinuity.
			cursor.Jump = didJump
			didJump = false

			// Set queue-depth metric and lag before blocking.
			r.readLag.Set(float64(time.Now().UnixNano()-
				cursor.Progress.MaxInclusive().Nanos()) / 1e9)
			r.readQueue.Set(float64(len(r.out)))

			// Non-blocking send of updated data.
			select {
			case r.out <- cursor:
			case <-ctx.Stopping():
				return
			}

			// Stop on error.
			if cursor.Error != nil {
				return
			}

			// There's more data to read, loop immediately with a
			// non-blocking bounds check.
			if cursor.Fragment {
				select {
				case <-boundsChanged:
					bounds, boundsChanged = r.bounds.Get()
					didJump = r.updateBounds(bounds)
				default:
				}
				continue
			}
		}
		// Blocking wait for the bounds to change.
		select {
		case <-boundsChanged:
			bounds, boundsChanged = r.bounds.Get()
			didJump = r.updateBounds(bounds)
		case <-ctx.Stopping():
			return
		}
	}
}

// nextCursor produces a value to send to the consumer.
func (r *tableReader) nextCursor(ctx *stopper.Context) *tableCursor {
	// Deal with database flakes.
	if err := retry.Retry(ctx, r.db, func(ctx context.Context) error {
		return r.queryOnce(ctx)
	}); err != nil {
		return &tableCursor{Error: err}
	}
	ret := &tableCursor{
		Batch: &types.MultiBatch{},
	}

	ret.Fragment = len(r.accumulator) >= r.fragmentSize
	if ret.Fragment {
		// Indicate partial progress within a larger batch, so we
		// can only advance the progress to before the timestamp
		// we're currently reading.
		lastMut := r.accumulator[len(r.accumulator)-1]
		r.scanBounds = hlc.RangeExcluding(lastMut.Time, r.scanBounds.Max())
		r.scanKey = lastMut.Key
		ret.Progress = hlc.RangeExcluding(hlc.Zero(), r.scanBounds.Min())
	} else {
		// We didn't hit the scan limit (or no rows were selected), so
		// we know that there's no data in the staging table whose
		// timestamp is less than the maximum we were using. We can
		// advance everything to that maximum timestamp.
		r.scanBounds = hlc.RangeEmptyAt(r.scanBounds.Max())
		r.scanKey = nil
		ret.Progress = hlc.RangeExcluding(hlc.Zero(), r.scanBounds.Max())
	}

	// Assemble the mutations into a complete batch.
	for idx, mut := range r.accumulator {
		var err error
		// The data retrieved from the database may have been
		// compressed. We want to decompress it outside the database
		// query to release the connection sooner.
		mut.Before, err = maybeGunzip(mut.Before)
		if err != nil {
			return &tableCursor{Error: err}
		}
		mut.Data, err = maybeGunzip(mut.Data)
		if err != nil {
			return &tableCursor{Error: err}
		}
		if err := ret.Batch.Accumulate(r.table, mut); err != nil {
			return &tableCursor{Error: err}
		}

		// Clear data from backing array.
		r.accumulator[idx] = types.Mutation{}
	}

	// Reset accumulator.
	r.accumulator = r.accumulator[:0]

	return ret
}

// queryOnce retrieves a limited number of rows to populate the reader's
// accumulator.
func (r *tableReader) queryOnce(ctx context.Context) error {
	start := time.Now()

	rows, err := r.db.Query(ctx,
		r.sqlQ,
		r.scanBounds.Min().Nanos(),
		r.scanBounds.Min().Logical(),
		r.scanKey,
		r.scanBounds.Max().Nanos(),
		r.scanBounds.Max().Logical(),
		r.fragmentSize)
	if err != nil {
		return errors.Wrap(err, r.sqlQ)
	}
	defer rows.Close()

	for rows.Next() {
		var mut types.Mutation
		var nanos int64
		var logical int
		if err := rows.Scan(&nanos, &logical, &mut.Key, &mut.Data, &mut.Before); err != nil {
			return errors.WithStack(err)
		}
		mut.Time = hlc.New(nanos, logical)

		r.accumulator = append(r.accumulator, mut)
	}
	if err := rows.Err(); err != nil {
		return errors.WithStack(err)
	}
	r.readCount.Add(float64(len(r.accumulator)))
	r.readDurations.Observe(time.Since(start).Seconds())
	return nil
}

// updateBounds ensures that the state of the reader can satisfy all
// reads within the requested bounds.
func (r *tableReader) updateBounds(proposed hlc.Range) (rewound bool) {
	// If the reader is new or idle, accept any value.
	if r.scanBounds.Empty() {
		r.scanBounds = proposed
		return true
	}
	// We only allow extensions of existing scans while running.
	if hlc.Compare(proposed.Max(), r.scanBounds.Max()) > 0 {
		next := hlc.RangeExcluding(r.scanBounds.Min(), proposed.Max())
		log.Tracef("read bounds %s: scan: %s proposed: %s next: %s",
			r.table, r.scanBounds, proposed, next)
		r.scanBounds = next
	}
	return false
}
