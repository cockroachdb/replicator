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
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
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

// Timestamps are less than and up to the resolved ones.
// For this $1 and $2 are previous resolved, $3 and $4 are the current
// resolved.
const sinkTableQueryRows = `
SELECT nanos, logical, key, after
FROM %s
WHERE ((nanos = $1 AND logical > $2) OR (nanos > $1)) AND
			((nanos = $3 AND logical <= $4) OR (nanos < $3))
`

const sinkTableDeleteRows = `
DELETE
FROM %s
WHERE ((nanos = $1 AND logical > $2) OR (nanos > $1)) AND
			((nanos = $3 AND logical <= $4) OR (nanos < $3))
`

// SinkTableFullName creates the conjoined db/table name to be used by the sink
// table.
func SinkTableFullName(resultDB string, resultTable string) string {
	return fmt.Sprintf("%s.%s_%s", *sinkDB, resultDB, resultTable)
}

// Line is used to parse a json line in the request body.
//{"after": {"a": 1, "b": 1}, "key": [1], "updated": "1585949214695218000.0000000000"}
type Line struct {
	// These are used for parsing the ndjson line.
	After   map[string]interface{} `json:"after"`
	Key     []interface{}          `json:"key"`
	Updated string                 `json:"updated"`

	// These are used for storing back to the sink table.
	nanos   int64
	logical int
	after   string
	key     string
}

// parseAFter parses the after column (retrieved from the sink table back into
// After so it can be used for upserting.
func (line *Line) parseAfter() error {
	// Parse the after columns
	// Large numbers are not turned into strings, so the UseNumber option for
	// the decoder is required.
	dec := json.NewDecoder(strings.NewReader(line.after))
	dec.UseNumber()
	return dec.Decode(&(line.After))
}

// getSinkTableValues is just the statements ordered as expected for the sink
// table insert statement.
func (line Line) getSinkTableValues() []interface{} {
	return []interface{}{line.nanos, line.logical, line.key, line.after}
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

// parseLine takes a single line from an ndjson and parses it into json then
// converts some of the components back to json for storage in the sink table.
// This parsing back and forth just seemed safer than manually parsing the line
// json.
func parseLine(rawBytes []byte) (Line, error) {
	var line Line

	// Large numbers are not turned into strings, so the UseNumber option for
	// the decoder is required.
	dec := json.NewDecoder(bytes.NewReader(rawBytes))
	dec.UseNumber()
	if err := dec.Decode(&line); err != nil {
		return Line{}, err
	}

	// Prase the timestamp into nanos and logical.
	var err error
	line.nanos, line.logical, err = parseSplitTimestamp(line.Updated)
	if err != nil {
		return Line{}, err
	}
	if line.nanos == 0 {
		return Line{}, fmt.Errorf("no nano component to the 'updated' timestamp field")
	}

	// Convert the after line back to json.
	afterBytes, err := json.Marshal(line.After)
	if err != nil {
		return Line{}, err
	}
	if len(afterBytes) == 0 {
		return Line{}, fmt.Errorf("no value present in 'after' field")
	}
	line.after = string(afterBytes)

	// Convert the key line back to json.
	if len(line.Key) <= 0 {
		return Line{}, fmt.Errorf("no value present in 'key' field")
	}
	keyBytes, err := json.Marshal(line.Key)
	if err != nil {
		return Line{}, err
	}
	if len(keyBytes) == 0 {
		return Line{}, fmt.Errorf("no value present in 'key' field")
	}
	line.key = string(keyBytes)
	return line, err
}

// CreateSinkTable creates if it does not exist, the a table used for sinking.
func CreateSinkTable(ctx context.Context, db *pgxpool.Pool, sinkTableFullName string) error {
	return Execute(ctx, db, fmt.Sprintf(sinkTableSchema, sinkTableFullName))
}

// WriteToSinkTable upserts all lines to the sink table via the COPY protocol.
func WriteToSinkTable(ctx context.Context, db *pgxpool.Pool, sinkTableFullName string, lines []Line) error {
	if len(lines) == 0 {
		return nil
	}
	parts := strings.Split(strings.ToLower(sinkTableFullName), ".")
	_, err := db.CopyFrom(ctx, parts,
		[]string{"nanos", "logical", "key", "after"},
		pgx.CopyFromSlice(len(lines), func(i int) ([]interface{}, error) {
			return lines[i].getSinkTableValues(), nil
		}))
	return errors.Wrapf(err, "writing to sink table %s", sinkTableFullName)
}

// FindAllRowsToUpdate returns all the rows that need to be updated from the
// sink table.
func FindAllRowsToUpdate(
	ctx context.Context, tx pgxtype.Querier, sinkTableFullName string, prev ResolvedLine, next ResolvedLine,
) ([]Line, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(sinkTableQueryRows, sinkTableFullName),
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

// DeleteSinkTableLines removes all line from the sinktable that have been processed
// based on the prev and next resolved line.
func DeleteSinkTableLines(
	ctx context.Context, tx pgxtype.Querier, sinkTableFullName string, prev ResolvedLine, next ResolvedLine,
) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(sinkTableDeleteRows, sinkTableFullName),
		prev.nanos, prev.logical, next.nanos, next.logical,
	)
	return err
}
