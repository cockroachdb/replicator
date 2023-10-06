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

package stage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type factory struct {
	db        *types.StagingPool
	stagingDB ident.Schema

	mu struct {
		sync.RWMutex
		instances *ident.TableMap[*stage]
	}
}

var _ types.Stagers = (*factory)(nil)

// Get returns a memoized instance of a stage for the given table.
func (f *factory) Get(ctx context.Context, target ident.Table) (types.Stager, error) {
	if ret := f.getUnlocked(target); ret != nil {
		return ret, nil
	}
	return f.createUnlocked(ctx, target)
}

func (f *factory) createUnlocked(ctx context.Context, table ident.Table) (*stage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances.GetZero(table); ret != nil {
		return ret, nil
	}

	ret, err := newStore(ctx, f.db, f.stagingDB, table)
	if err == nil {
		f.mu.instances.Put(table, ret)
	}
	return ret, err
}

func (f *factory) getUnlocked(table ident.Table) *stage {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances.GetZero(table)
}

// SelectMany implements types.Stagers.
func (f *factory) SelectMany(
	ctx context.Context,
	tx types.StagingQuerier,
	q *types.SelectManyCursor,
	fn types.SelectManyCallback,
) error {
	if q.Limit == 0 {
		return errors.New("limit must be set")
	}
	// Ensure offset >= start.
	if hlc.Compare(q.OffsetTime, q.Start) < 0 {
		q.OffsetTime = q.Start
	}

	// Each table is assigned a numeric id to simplify the queries.
	tablesToIds := &ident.TableMap[int]{}
	idsToTables := make(map[int]ident.Table)
	var orderedTables []ident.Table
	for _, tables := range q.Targets {
		orderedTables = append(orderedTables, tables...)
		for _, table := range tables {
			id := tablesToIds.Len()
			if _, duplicate := tablesToIds.Get(table); duplicate {
				return errors.Errorf("duplicate table name: %s", table)
			}
			tablesToIds.Put(table, id)
			idsToTables[id] = table
		}
	}

	// Define the rows variable here, so we don't leak it from the
	// variety of error code-paths below.
	var rows pgx.Rows
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()

	for {
		start := time.Now()

		offsetTableIdx := -1
		if !q.OffsetTable.Empty() {
			offsetTableIdx = tablesToIds.GetZero(q.OffsetTable)
		}

		// This is a union-all construct:
		//   WITH data AS (
		//     (SELECT ... FROM staging_table)
		//     UNION ALL
		//     (SELECT ... FROM another_staging_table)
		//   )
		//   SELECT ... FROM data ....
		//
		// The generated SQL does vary based on the offset table value,
		// so we can't easily reuse it across iterations.
		var sb strings.Builder
		sb.WriteString(`WITH
args AS (SELECT $1::INT, $2::INT, $3::INT, $4::INT, $5::INT, $6:::INT, $7::INT, $8::STRING),
data AS (`)
		needsUnion := false
		for _, table := range orderedTables {
			id := tablesToIds.GetZero(table)
			// If we're backfilling, the dominant ordering term is the
			// table. Any tables that are before the starting table can
			// simply be elided from the query.
			if q.Backfill && id < offsetTableIdx {
				continue
			}
			if needsUnion {
				sb.WriteString(" UNION ALL ")
			} else {
				needsUnion = true
			}

			// Mangle the real table name into its staging table name.
			destination := stagingTable(f.stagingDB, table)

			// This ORDER BY should be driven by the index scan, but we
			// don't want to be subject to any cross-range scan merges.
			// The WHERE clauses in the sub-queries should line up with
			// the ORDER BY in the top-level that we use for pagination.
			if q.Backfill {
				if id == offsetTableIdx {
					// We're resuming in the middle of reading a table.
					_, _ = fmt.Fprintf(&sb,
						"(SELECT %[1]d AS t, nanos, logical, key, mut, before "+
							"FROM %[2]s "+
							"WHERE (nanos, logical, key) > ($6, $7, $8) "+
							"AND (nanos, logical) <= ($3, $4) "+
							"ORDER BY nanos, logical, key "+
							"LIMIT $5)",
						id, destination)
				} else {
					// id must be > offsetTableIdx, since we filter out
					// lesser values above. This means that we haven't
					// yet read any values from this table, so we want
					// to start at the beginning time.
					_, _ = fmt.Fprintf(&sb,
						"(SELECT %[1]d AS t, nanos, logical, key, mut, before "+
							"FROM %[2]s "+
							"WHERE (nanos, logical) >= ($1, $2) "+
							"AND (nanos, logical) <= ($3, $4) "+
							"ORDER BY nanos, logical, key "+
							"LIMIT $5)",
						id, destination)
				}
			} else {
				// The differences in these cases have to do with the
				// comparison operators around (nanos,logical) to pick the
				// correct starting point for the scan.
				if id == offsetTableIdx {
					_, _ = fmt.Fprintf(&sb,
						"(SELECT %[1]d AS t, nanos, logical, key, mut, before "+
							"FROM %[2]s "+
							"WHERE ( (nanos, logical, key) > ($6, $7, $8) ) "+
							"AND (nanos, logical) <= ($3, $4) "+
							"ORDER BY nanos, logical, key "+
							"LIMIT $5)",
						id, destination)
				} else if id > offsetTableIdx {
					_, _ = fmt.Fprintf(&sb,
						"(SELECT %[1]d AS t, nanos, logical, key, mut, before "+
							"FROM %[2]s "+
							"WHERE (nanos, logical) >= ($6, $7) "+
							"AND (nanos, logical) <= ($3, $4) "+
							"ORDER BY nanos, logical, key "+
							"LIMIT $5)",
						id, destination)
				} else {
					_, _ = fmt.Fprintf(&sb,
						"(SELECT %[1]d AS t, nanos, logical, key, mut, before "+
							"FROM %[2]s "+
							"WHERE (nanos, logical) > ($6, $7) "+
							"AND (nanos, logical) <= ($3, $4) "+
							"ORDER BY nanos, logical, key "+
							"LIMIT $5)",
						id, destination)
				}
			}
		}
		sb.WriteString(`) SELECT t, nanos, logical, key, mut, before FROM data `)
		if q.Backfill {
			sb.WriteString(`ORDER BY t, nanos, logical, key LIMIT $5`)
		} else {
			sb.WriteString(`ORDER BY nanos, logical, t, key LIMIT $5`)
		}

		var err error
		// Rows closed in defer statement above.
		rows, err = tx.Query(ctx, sb.String(),
			q.Start.Nanos(),
			q.Start.Logical(),
			q.End.Nanos(),
			q.End.Logical(),
			q.Limit,
			q.OffsetTime.Nanos(),
			q.OffsetTime.Logical(),
			string(q.OffsetKey),
		)
		if err != nil {
			return errors.Wrap(err, sb.String())
		}

		count := 0
		var lastMut types.Mutation
		var lastTable ident.Table
		for rows.Next() {
			count++
			var mut types.Mutation
			var tableIdx int
			var nanos int64
			var logical int
			if err := rows.Scan(&tableIdx, &nanos, &logical, &mut.Key, &mut.Data, &mut.Before); err != nil {
				return errors.WithStack(err)
			}
			mut.Before, err = maybeGunzip(mut.Before)
			if err != nil {
				return err
			}
			mut.Data, err = maybeGunzip(mut.Data)
			if err != nil {
				return err
			}
			mut.Time = hlc.New(nanos, logical)

			lastMut = mut
			lastTable = idsToTables[tableIdx]
			if err := fn(ctx, lastTable, mut); err != nil {
				return err
			}
		}

		// Only update the cursor if we've successfully read the entire page.
		q.OffsetKey = lastMut.Key
		q.OffsetTime = lastMut.Time
		q.OffsetTable = lastTable

		log.WithFields(log.Fields{
			"count":    count,
			"duration": time.Since(start),
			"targets":  q.Targets,
		}).Debug("retrieved staged mutations")

		// We can stop once we've received less than a max-sized window
		// of mutations.  Otherwise, loop around.
		if count < q.Limit {
			return nil
		}
	}
}
