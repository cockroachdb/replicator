// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package stage defines a means of storing and retrieving mutations
// to be applied to a table.
package stage

// The code in this file is reworked from sink_table.go.

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// stage implements a storage and retrieval mechanism for staging
// Mutation instances.
type stage struct {
	// The staging table that holds the mutations.
	stage ident.Table

	// Compute SQL fragments exactly once on startup.
	sql struct {
		drain string // drain rows from the staging table
		store string // store mutations
	}
}

var _ types.Stager = (*stage)(nil)

// newStore constructs a new mutation stage that will track pending
// mutations to be applied to the given target table.
func newStore(
	ctx context.Context, db pgxtype.Querier, stagingDB ident.Ident, target ident.Table,
) (*stage, error) {
	mangledName := strings.Join(
		[]string{target.Database().Raw(), target.Schema().Raw(), target.Table().Raw()}, "_")
	table := ident.NewTable(stagingDB, ident.Public, ident.New(mangledName))

	if err := retry.Execute(ctx, db, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	nanos INT NOT NULL,
  logical INT NOT NULL,
	  key STRING NOT NULL,
	  mut JSONB NOT NULL,
	PRIMARY KEY (nanos, logical, key)
)`, table)); err != nil {
		return nil, err
	}

	s := &stage{stage: table}

	s.sql.drain = fmt.Sprintf(drainTemplate, table)
	s.sql.store = fmt.Sprintf(putTemplate, table)

	return s, nil
}

const drainTemplate = `
WITH d AS (DELETE FROM %s
WHERE (nanos, logical) BETWEEN ($1, $2) AND ($3, $4)
RETURNING nanos, logical, key, mut)
SELECT DISTINCT ON (key) nanos, logical, key, mut FROM d
ORDER BY key ASC, nanos DESC, logical DESC
`

// Drain dequeues mutations between the given timestamps.
func (s *stage) Drain(
	ctx context.Context, tx pgxtype.Querier, prev, next hlc.Time,
) ([]types.Mutation, error) {
	var ret []types.Mutation
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.Query(ctx, s.sql.drain,
			prev.Nanos(), prev.Logical(), next.Nanos(), next.Logical(),
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		// Clear any previous loop, but save the backing array.
		ret = ret[:0]
		for rows.Next() {
			var mut types.Mutation
			var nanos int64
			var logical int
			if err := rows.Scan(&nanos, &logical, &mut.Key, &mut.Data); err != nil {
				return err
			}
			mut.Time = hlc.New(nanos, logical)
			ret = append(ret, mut)
		}
		return nil
	})
	return ret, errors.Wrapf(err, "drain %s [%s, %s]", s.stage, prev, next)
}

// Arrays of JSONB aren't implemented
// https://github.com/cockroachdb/cockroach/issues/23468
const putTemplate = `UPSERT INTO %s (nanos, logical, key, mut) VALUES ($1, $2, $3, $4)`

// Store stores some number of Mutations into the database.
func (s *stage) Store(ctx context.Context, db types.Batcher, mutations []types.Mutation) error {
	return batches.Batch(len(mutations), func(begin, end int) error {
		return s.putOne(ctx, db, mutations[begin:end])
	})
}

func (s *stage) putOne(ctx context.Context, db types.Batcher, mutations []types.Mutation) error {
	batch := &pgx.Batch{}

	for _, mut := range mutations {
		var jsonText string
		if mut.IsDelete() {
			jsonText = "null"
		} else {
			jsonText = string(mut.Data)
		}

		batch.Queue(s.sql.store,
			mut.Time.Nanos(),
			mut.Time.Logical(),
			string(mut.Key),
			jsonText)
	}

	res := db.SendBatch(ctx, batch)
	defer res.Close()

	for i, j := 0, batch.Len(); i < j; i++ {
		if _, err := res.Exec(); err != nil {
			return errors.Wrap(err, s.sql.store)
		}
	}

	log.Printf("staged %d entries for %s", len(mutations), s.stage)
	return nil
}
