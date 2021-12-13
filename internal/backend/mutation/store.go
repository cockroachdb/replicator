// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package mutation defines a means of storing and retrieving mutations
// to be applied to a table.
package mutation

// The code in this file is reworked from sink_table.go.

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
)

// store implements a storage and retrieval mechanism for staging
// Mutation instances.
type store struct {
	// The staging table that holds the mutations.
	stage ident.Table

	// Compute SQL fragments exactly once on startup.
	sql struct {
		drain string // drain rows from the staging table
		store string // store mutations
	}
}

var _ sinktypes.MutationStore = (*store)(nil)

// newStore constructs a new mutation store that will track pending
// mutations to be applied to the given target table.
func newStore(
	ctx context.Context, db pgxtype.Querier, stagingDb ident.Ident, target ident.Table,
) (*store, error) {
	mangledName := "_" + strings.Join(
		[]string{target.Database().Raw(), target.Schema().Raw(), target.Table().Raw()}, "_")
	stage := ident.NewTable(stagingDb, ident.Public, ident.New(mangledName))

	if err := retry.Execute(ctx, db, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	nanos INT NOT NULL,
  logical INT NOT NULL,
	  key STRING NOT NULL,
	  mut JSONB NOT NULL,
	PRIMARY KEY (nanos, logical, key)
)`, stage)); err != nil {
		return nil, err
	}

	s := &store{stage: stage}

	s.sql.drain = fmt.Sprintf(drainTemplate, stage)
	s.sql.store = fmt.Sprintf(putTemplate, stage)

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
func (s *store) Drain(
	ctx context.Context, tx pgxtype.Querier, prev, next hlc.Time,
) ([]sinktypes.Mutation, error) {
	var ret []sinktypes.Mutation
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
			var mut sinktypes.Mutation
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
const putTemplate = `
UPSERT INTO %s (nanos, logical, key, mut)
SELECT unnest($1::INT8[]), unnest($2::INT8[]), unnest($3::STRING[]), unnest($4::STRING[])::JSONB`

// Store stores some number of Mutations into the database.
func (s *store) Store(
	ctx context.Context, tx pgxtype.Querier, mutations []sinktypes.Mutation,
) error {
	return batches.Batch(len(mutations), func(begin, end int) error {
		return s.putOne(ctx, tx, mutations[begin:end])
	})
}

func (s *store) putOne(
	ctx context.Context, tx pgxtype.Querier, mutations []sinktypes.Mutation,
) error {
	nanos, r := batches.Int64()
	defer r()
	logicals, r := batches.Int()
	defer r()
	keys, r := batches.String()
	defer r()
	datas, r := batches.String()
	defer r()

	for i := range mutations {
		nanos = append(nanos, mutations[i].Time.Nanos())
		logicals = append(logicals, mutations[i].Time.Logical())
		keys = append(keys, string(mutations[i].Key))
		if mutations[i].Delete() {
			datas = append(datas, "null")
		} else {
			datas = append(datas, string(mutations[i].Data))
		}
	}

	err := retry.Execute(ctx, tx, s.sql.store, nanos, logicals, keys, datas)
	if err == nil {
		log.Printf("staged %d entries for %s", len(mutations), s.stage)
	}
	return errors.Wrapf(err, "store %s", s.stage)
}
