// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stage

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type factory struct {
	db        *pgxpool.Pool
	stagingDB ident.Ident

	mu struct {
		sync.RWMutex
		instances map[ident.Table]*stage
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

	if ret := f.mu.instances[table]; ret != nil {
		return ret, nil
	}

	ret, err := newStore(ctx, f.db, f.stagingDB, table)
	if err == nil {
		f.mu.instances[table] = ret
	}
	return ret, err
}

func (f *factory) getUnlocked(table ident.Table) *stage {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances[table]
}

// SelectMany implements types.Stagers.
func (f *factory) SelectMany(
	ctx context.Context, tx pgxtype.Querier, q *types.SelectManyCursor,
) (map[ident.Table][]types.Mutation, bool, error) {
	start := time.Now()
	if q.Limit == 0 {
		return nil, false, errors.New("limit must be set")
	}
	// Ensure offset >= start.
	if hlc.Compare(q.OffsetTime, q.Start) < 0 {
		q.OffsetTime = q.Start
	}

	// Each table is assigned a numeric id to simplify the queries.
	offsetTableIdx := -1
	tablesToIds := make(map[ident.Table]int)
	idsToTables := make(map[int]ident.Table)
	var orderedTables []ident.Table
	for _, tables := range q.Targets {
		orderedTables = append(orderedTables, tables...)
		for _, table := range tables {
			id := len(tablesToIds)
			if _, duplicate := tablesToIds[table]; duplicate {
				return nil, false, errors.Errorf("duplicate table name: %s", table)
			}
			tablesToIds[table] = id
			idsToTables[id] = table
			if table == q.OffsetTable {
				offsetTableIdx = id
			}
		}
	}

	// This is a union-all construct:
	//   WITH data AS (
	//     (SELECT ... FROM staging_table)
	//     UNION ALL
	//     (SELECT ... FROM another_staging_table)
	//   )
	//   SELECT ... FROM data ....
	var sb strings.Builder
	sb.WriteString(`WITH
args AS (SELECT $1::INT, $2::INT, $3::INT, $4::INT, $5::INT, $6:::INT, $7::INT, $8::STRING),
data AS (`)
	needsUnion := false
	for _, table := range orderedTables {
		id := tablesToIds[table]
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
		stagingTable := ident.NewTable(
			f.stagingDB, ident.Public, ident.New(mangleTableName(table)))

		// This ORDER BY should be driven by the index scan, but we
		// don't want to be subject to any cross-range scan merges.
		// The WHERE clauses in the sub-queries should line up with
		// the ORDER BY in the top-level that we use for pagination.
		if q.Backfill {
			if id == offsetTableIdx {
				// We're resuming in the middle of reading a table.
				_, _ = fmt.Fprintf(&sb,
					"(SELECT %[1]d AS t, nanos, logical, key, mut "+
						"FROM %[2]s "+
						"WHERE (nanos, logical, key) > ($6, $7, $8) "+
						"AND (nanos, logical) <= ($3, $4) "+
						"ORDER BY nanos, logical, key "+
						"LIMIT $5)",
					id, stagingTable)
			} else {
				// id must be > offsetTableIdx, since we filter out
				// lesser values above. This means that we haven't
				// yet read any values from this table, so we want
				// to start at the beginning time.
				_, _ = fmt.Fprintf(&sb,
					"(SELECT %[1]d AS t, nanos, logical, key, mut "+
						"FROM %[2]s "+
						"WHERE (nanos, logical) >= ($1, $2) "+
						"AND (nanos, logical) <= ($3, $4) "+
						"ORDER BY nanos, logical, key "+
						"LIMIT $5)",
					id, stagingTable)
			}
		} else {
			// The differences in these cases have to do with the
			// comparison operators around (nanos,logical) to pick the
			// correct starting point for the scan.
			if id == offsetTableIdx {
				_, _ = fmt.Fprintf(&sb,
					"(SELECT %[1]d AS t, nanos, logical, key, mut "+
						"FROM %[2]s "+
						"WHERE ( (nanos, logical, key) > ($6, $7, $8) ) "+
						"AND (nanos, logical) <= ($3, $4) "+
						"ORDER BY nanos, logical, key "+
						"LIMIT $5)",
					id, stagingTable)
			} else if id > offsetTableIdx {
				_, _ = fmt.Fprintf(&sb,
					"(SELECT %[1]d AS t, nanos, logical, key, mut "+
						"FROM %[2]s "+
						"WHERE (nanos, logical) >= ($6, $7) "+
						"AND (nanos, logical) <= ($3, $4) "+
						"ORDER BY nanos, logical, key "+
						"LIMIT $5)",
					id, stagingTable)
			} else {
				_, _ = fmt.Fprintf(&sb,
					"(SELECT %[1]d AS t, nanos, logical, key, mut "+
						"FROM %[2]s "+
						"WHERE (nanos, logical) > ($6, $7) "+
						"AND (nanos, logical) <= ($3, $4) "+
						"ORDER BY nanos, logical, key "+
						"LIMIT $5)",
					id, stagingTable)
			}
		}
	}
	sb.WriteString(`) SELECT t, nanos, logical, key, mut FROM data `)
	if q.Backfill {
		sb.WriteString(`ORDER BY t, nanos, logical, key LIMIT $5`)
	} else {
		sb.WriteString(`ORDER BY nanos, logical, t, key LIMIT $5`)
	}

	rows, err := tx.Query(ctx, sb.String(),
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
		return nil, false, errors.Wrap(err, sb.String())
	}
	defer rows.Close()

	count := 0
	var lastTable ident.Table
	var lastMut types.Mutation
	ret := make(map[ident.Table][]types.Mutation, len(idsToTables))
	for rows.Next() {
		count++
		var mut types.Mutation
		var tableIdx int
		var nanos int64
		var logical int
		if err := rows.Scan(&tableIdx, &nanos, &logical, &mut.Key, &mut.Data); err != nil {
			return nil, false, errors.WithStack(err)
		}
		mut.Time = hlc.New(nanos, logical)

		// Check for gzip magic numbers.
		if len(mut.Data) >= 2 && mut.Data[0] == 0x1f && mut.Data[1] == 0x8b {
			r, err := gzip.NewReader(bytes.NewReader(mut.Data))
			if err != nil {
				return nil, false, errors.WithStack(err)
			}
			mut.Data, err = io.ReadAll(r)
			if err != nil {
				return nil, false, errors.WithStack(err)
			}
		}

		var ok bool
		lastTable, ok = idsToTables[tableIdx]
		if !ok {
			return nil, false, errors.Errorf("received unexpected table id in query %d", tableIdx)
		}
		ret[lastTable] = append(ret[lastTable], mut)
		lastMut = mut
	}

	q.OffsetKey = lastMut.Key
	q.OffsetTime = lastMut.Time
	q.OffsetTable = lastTable

	log.WithFields(log.Fields{
		"count":    count,
		"duration": time.Since(start),
		"targets":  q.Targets,
	}).Debug("retrieved staged mutations")

	return ret, count >= q.Limit, nil
}
