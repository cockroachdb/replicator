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

	// Each table is assigned a numeric id to simplify the queries.
	afterTableIdx := -1
	afterGroupIdx := -1
	tableIds := make(map[int]ident.Table)

	// This is a union-all construct:
	//   WITH data AS (
	//     (SELECT ... FROM staging_table)
	//     UNION ALL
	//     (SELECT ... FROM another_staging_table)
	//   )
	//   SELECT ... FROM data ....
	var sb strings.Builder
	sb.WriteString("WITH data AS (")
	for groupIdx, tables := range q.Targets {
		for _, table := range tables {
			// Assign each table a unique id for the purposes of this query.
			id := len(tableIds)
			if _, duplicate := tableIds[id]; duplicate {
				return nil, false, errors.Errorf("duplicate table name: %s", table)
			}
			tableIds[id] = table
			if id > 0 {
				sb.WriteString(" UNION ALL ")
			}

			// Update cursor values if this is the table to start from.
			if table == q.AfterTable {
				afterTableIdx = id
				afterGroupIdx = groupIdx
			}

			// Mangle the real table name into its staging table name.
			stagingTable := ident.NewTable(
				f.stagingDB, ident.Public, ident.New(mangleTableName(table)))

			// This ORDER BY should be driven by the index scan, but we
			// don't want to be subject to any cross-range scan merges.
			_, _ = fmt.Fprintf(&sb,
				"(SELECT %d AS g, %d AS t, nanos, logical, key, mut FROM %s ORDER BY nanos, logical, key)",
				groupIdx, id, stagingTable)
		}
	}
	sb.WriteString(`) SELECT t, nanos, logical, key, mut FROM data`)
	if q.Backfill {
		sb.WriteString(`
WHERE (g, t, nanos, logical, key) > ($3, $4, $1, $2, $5) AND (nanos, logical) <= ($6, $7)
ORDER BY g, t, nanos, logical, key
LIMIT $8
`)
	} else {
		sb.WriteString(`
WHERE (nanos, logical, g, t, key) > ($1, $2, $3, $4, $5) AND (nanos, logical) <= ($6, $7)
ORDER BY nanos, logical, g, t, key
LIMIT $8
`)
	}

	rows, err := tx.Query(ctx, sb.String(),
		q.AfterTime.Nanos(),
		q.AfterTime.Logical(),
		afterGroupIdx,
		afterTableIdx,
		string(q.AfterKey),
		q.Until.Nanos(),
		q.Until.Logical(),
		q.Limit,
	)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	defer rows.Close()

	count := 0
	var lastTable ident.Table
	var lastMut types.Mutation
	ret := make(map[ident.Table][]types.Mutation, len(tableIds))
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
		lastTable, ok = tableIds[tableIdx]
		if !ok {
			return nil, false, errors.Errorf("received unexpected table id in query %d", tableIdx)
		}
		ret[lastTable] = append(ret[lastTable], mut)
		lastMut = mut
	}

	q.AfterKey = lastMut.Key
	q.AfterTime = lastMut.Time
	q.AfterTable = lastTable

	log.WithFields(log.Fields{
		"count":    count,
		"duration": time.Since(start),
		"targets":  q.Targets,
	}).Debug("retrieved staged mutations")

	return ret, count >= q.Limit, nil
}
