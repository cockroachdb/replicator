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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
)

const sinkTableSchema = `
CREATE TABLE IF NOT EXISTS %s (
	nanos INT NOT NULL,
  logical INT NOT NULL,
	key STRING NOT NULL,
	after STRING,
	PRIMARY KEY (nanos, logical, key)
)
`

const sinkTableWrite = `UPSERT INTO %s (nanos, logical, key, after) VALUES `

// Timestamps are less than and up to the resolved ones.
// For this $1 and $2 are previous resolved, $3 and $4 are the current
// resolved.
const sinkTableDrainRows = `
DELETE
FROM %s
WHERE ((nanos = $1 AND logical > $2) OR (nanos > $1)) AND
			((nanos = $3 AND logical <= $4) OR (nanos < $3))
RETURNING nanos, logical, key, after
`

// SinkTableFullName creates the conjoined db/table name to be used by the sink
// table.
func SinkTableFullName(resultDB string, resultTable string) string {
	return fmt.Sprintf("%s.%s_%s", *sinkDB, resultDB, resultTable)
}

// Line stores pending mutations.
type Line struct {
	after   json.RawMessage // The mutations to apply: {"a": 1, "b": 1}
	key     json.RawMessage // Primary key values: [1, 2]
	nanos   int64           // HLC time base
	logical int             // HLC logical counter
}

// extractColumns parses the keys from the "after" payload block and
// appends them to the given slice.
func (line *Line) extractColumns(into []string) ([]string, error) {
	m := make(map[string]json.RawMessage)
	dec := json.NewDecoder(bytes.NewReader(line.after))
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	for k := range m {
		into = append(into, k)
	}
	return into, nil
}

// parseAfter reifies the mutations to be applied.
func (line *Line) parseAfter(into map[string]interface{}) error {
	// Parse the after columns
	// Large numbers are not turned into strings, so the UseNumber option for
	// the decoder is required.
	dec := json.NewDecoder(bytes.NewReader(line.after))
	dec.UseNumber()
	return dec.Decode(&into)
}

// getSinkTableValues is just the statements ordered as expected for the sink
// table insert statement.
func (line Line) getSinkTableValues() []interface{} {
	return []interface{}{line.nanos, line.logical, string(line.key), string(line.after)}
}

// parseSplitTimestamp splits a timestmap of tte format NNNN.LLL into an int64
// for the nanos and an int for the logical component.
func parseSplitTimestamp(timestamp string) (int64, int, error) {
	splits := strings.Split(timestamp, ".")
	if len(splits) != 2 {
		return 0, 0, fmt.Errorf("can't parse timestamp %s", timestamp)
	}
	nanos, err := strconv.ParseInt(splits[0], 0, 0)
	if err != nil {
		return 0, 0, err
	}
	if nanos <= 0 {
		return 0, 0, fmt.Errorf("nanos must be greater than 0: %d", nanos)
	}
	logical, err := strconv.Atoi(splits[1])
	if err != nil {
		return 0, 0, err
	}
	return nanos, logical, nil
}

// parseLine takes a single line from an ndjson and extracts enough
// information to be able to persist it to the staging table.
func parseLine(rawBytes []byte) (Line, error) {
	var payload struct {
		After   json.RawMessage `json:"after"`
		Key     json.RawMessage `json:"key"`
		Updated string          `json:"updated"`
	}

	// Large numbers are not turned into strings, so the UseNumber option for
	// the decoder is required.
	dec := json.NewDecoder(bytes.NewReader(rawBytes))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return Line{}, err
	}

	// Parse the timestamp into nanos and logical.
	nanos, logical, err := parseSplitTimestamp(payload.Updated)
	if err != nil {
		return Line{}, err
	}
	if nanos == 0 {
		return Line{}, fmt.Errorf("no nano component to the 'updated' timestamp field")
	}

	return Line{
		after:   payload.After,
		key:     payload.Key,
		logical: logical,
		nanos:   nanos,
	}, nil
}

// CreateSinkTable creates if it does not exist, the a table used for sinking.
func CreateSinkTable(ctx context.Context, db *pgxpool.Pool, sinkTableFullName string) error {
	return Execute(ctx, db, fmt.Sprintf(sinkTableSchema, sinkTableFullName))
}

// WriteToSinkTable upserts all lines to the sink table. Never submit more than
// 10,000 lines to this function at a time.
func WriteToSinkTable(ctx context.Context, db *pgxpool.Pool, sinkTableFullName string, lines []Line) error {
	if len(lines) == 0 {
		return nil
	}
	var statement strings.Builder
	if _, err := fmt.Fprintf(&statement, sinkTableWrite, sinkTableFullName); err != nil {
		return err
	}
	var values []interface{}
	for i, line := range lines {
		values = append(values, line.getSinkTableValues()...)
		if i == 0 {
			if _, err := fmt.Fprint(&statement, "($1,$2,$3,$4)"); err != nil {
				return err
			}
		} else {
			j := i * 4
			if _, err := fmt.Fprintf(&statement, ",($%d,$%d,$%d,$%d)", j+1, j+2, j+3, j+4); err != nil {
				return err
			}
		}
	}

	return Execute(ctx, db, statement.String(), values...)
}

// DrainAllRowsToUpdate deletes and returns the rows that need to be
// updated from the sink table.
func DrainAllRowsToUpdate(
	ctx context.Context, tx pgxtype.Querier, sinkTableFullName string, prev ResolvedLine, next ResolvedLine,
) ([]Line, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(sinkTableDrainRows, sinkTableFullName),
		prev.nanos, prev.logical, next.nanos, next.logical,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lines []Line
	var line Line
	for rows.Next() {
		rows.Scan(&(line.nanos), &(line.logical), &(line.key), &(line.after))
		lines = append(lines, line)
	}
	return lines, nil
}
