// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const resolvedTableSchema = `
CREATE TABLE IF NOT EXISTS %s (
	endpoint STRING PRIMARY KEY,
	nanos INT NOT NULL,
	logical INT NOT NULL
)
`

// Make this an option?
const resolvedTableName = `_release`

const resolvedTableQuery = `SELECT endpoint, nanos, logical FROM %s WHERE endpoint = $1`

const resolvedTableWrite = `UPSERT INTO %s (endpoint, nanos, logical) VALUES ($1, $2, $3)`

func resolvedFullTableName() string {
	return fmt.Sprintf("%s.%s", *sinkDB, resolvedTableName)
}

// CreateResolvedTable creates a release table if none exists.
func CreateResolvedTable(ctx context.Context, db *pgxpool.Pool) error {
	return Execute(ctx, db, fmt.Sprintf(resolvedTableSchema, resolvedFullTableName()))
}

// ResolvedLine is used to parse a json line in the request body of a resolved
// message.
type ResolvedLine struct {
	// These are use for parsing the resolved line.
	Resolved string `json:"resolved"`

	// There are used for storing back into the resolved table.
	nanos    int64
	logical  int
	endpoint string
}

func parseResolvedLine(rawBytes []byte, endpoint string) (ResolvedLine, error) {
	resolvedLine := ResolvedLine{
		endpoint: endpoint,
	}
	json.Unmarshal(rawBytes, &resolvedLine)

	// Prase the timestamp into nanos and logical.
	var err error
	resolvedLine.nanos, resolvedLine.logical, err = parseSplitTimestamp(resolvedLine.Resolved)
	if err != nil {
		return ResolvedLine{}, err
	}
	if resolvedLine.nanos == 0 {
		return ResolvedLine{}, fmt.Errorf("no nano component to the 'updated' timestamp field")
	}

	return resolvedLine, nil
}

// getPreviousResolvedTimestamp returns the last recorded resolved for a
// specific endpoint.
func getPreviousResolved(ctx context.Context, tx pgxtype.Querier, endpoint string) (ResolvedLine, error) {
	// Needs retry.
	var resolvedLine ResolvedLine
	err := tx.QueryRow(ctx,
		fmt.Sprintf(resolvedTableQuery, resolvedFullTableName()), endpoint,
	).Scan(&(resolvedLine.endpoint), &(resolvedLine.nanos), &(resolvedLine.logical))
	switch err {
	case pgx.ErrNoRows:
		// No line exists yet, go back to the start of time.
		return ResolvedLine{endpoint: endpoint}, nil
	case nil:
		// Found the line.
		return resolvedLine, nil
	default:
		return ResolvedLine{}, err
	}
}

// Writes the updated timestamp to the resolved table.
func (rl ResolvedLine) writeUpdated(ctx context.Context, tx pgxtype.Querier) error {
	// Needs retry.
	_, err := tx.Exec(ctx, fmt.Sprintf(resolvedTableWrite, resolvedFullTableName()),
		rl.endpoint, rl.nanos, rl.logical,
	)
	return err
}
