// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package timestamp implements a simple key-timestamp store.
package timestamp

// The code in this file is adapted from resolved_table.go

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// DefaultTable is a default table name to pass to New.
var DefaultTable = ident.NewTable(ident.StagingDb, ident.Public, ident.New("_timestamps"))

// store implements a simple key/value store for HLC timestamps.
type store struct {
	sql struct {
		swap string
	}
}

var _ sinktypes.TimeSwapper = (*store)(nil)

// New constructs a store using the specified table for storage.
func New(
	ctx context.Context, tx pgxtype.Querier, target ident.Table,
) (sinktypes.TimeSwapper, error) {
	if err := retry.Execute(ctx, tx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
key STRING NOT NULL PRIMARY KEY,
nanos INT8 NOT NULL,
logical INT8 NOT NULL
)
`, target)); err != nil {
		return nil, errors.WithStack(err)
	}

	ret := &store{}
	ret.sql.swap = fmt.Sprintf(swapTemplate, target)

	return ret, nil
}

const swapTemplate = `
WITH u AS (UPSERT INTO %[1]s (nanos, logical, key) VALUES ($1, $2, $3) RETURNING 0)
SELECT nanos, logical FROM %[1]s WHERE key=$3`

// Swap updates the value associated with the key, returning the
// previous value.
func (s *store) Swap(
	ctx context.Context, db pgxtype.Querier, key string, value hlc.Time,
) (hlc.Time, error) {
	var nanos int64
	var logical int
	err := retry.Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(
			ctx,
			s.sql.swap,
			value.Nanos(),
			value.Logical(),
			key).Scan(&nanos, &logical)
	})
	// No rows means that we haven't seen this key before.
	if errors.Is(err, pgx.ErrNoRows) {
		return hlc.Zero(), nil
	}
	return hlc.New(nanos, logical), errors.Wrap(err, s.sql.swap)
}
