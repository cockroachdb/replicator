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
	"fmt"
	"math"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

// findAllRowsToUpdateDB is a wrapper around FindAllRowsToUpdate that handles
// the transaction for testing.
func findAllRowsToUpdateDB(
	ctx context.Context, db *pgxpool.Pool, sinkTableFullName string, prev ResolvedLine, next ResolvedLine,
) ([]Line, error) {
	var lines []Line

	if err := Retry(ctx, func(ctx context.Context) error {
		var err error
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		lines, err = DrainAllRowsToUpdate(ctx, tx, sinkTableFullName, prev, next)
		return err
	}); err != nil {
		return nil, err
	}
	return lines, nil
}

func TestParseSplitTimestamp(t *testing.T) {
	tests := []struct {
		testcase        string
		expectedPass    bool
		expectedNanos   int64
		expectedLogical int
	}{
		{"", false, 0, 0},
		{".", false, 0, 0},
		{"1233", false, 0, 0},
		{".1233", false, 0, 0},
		{"123.123", true, 123, 123},
		{"0.0", false, 0, 0},
		{"1586019746136571000.0000000000", true, 1586019746136571000, 0},
		{"1586019746136571000.0000000001", true, 1586019746136571000, 1},
		{"9223372036854775807.2147483647", true, math.MaxInt64, math.MaxInt32},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d - %s", i, test.testcase), func(t *testing.T) {
			actualNanos, actualLogical, actualErr := parseSplitTimestamp(test.testcase)
			if test.expectedPass == (actualErr != nil) {
				t.Errorf("Expected %v, got %s", test.expectedPass, actualErr)
			}
			if test.expectedNanos != actualNanos {
				t.Errorf("Expected %d nanos, got %d nanos", test.expectedNanos, actualNanos)
			}
			if test.expectedLogical != actualLogical {
				t.Errorf("Expected %d nanos, got %d nanos", test.expectedLogical, actualLogical)
			}
		})
	}
}

func TestParseLine(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testcase        string
		expectedPass    bool
		expectedAfter   string
		expectedKey     string
		expectedNanos   int64
		expectedLogical int
	}{
		{
			`{"after": {"a": 9, "b": 9}, "key": [9], "updated": "1586020760120222000.0000000000"}`,
			true, `{"a":9,"b":9}`, `[9]`, 1586020760120222000, 0,
		},
		{
			`{"after": {"a": 9, "b": 9}, "key": [9]`,
			false, "", "", 0, 0,
		},
		{
			`{"after": {"a": 9, "b": 9}, "key": [9], "updated": "1586020760120222000"}`,
			false, "", "", 0, 0,
		},
		{
			`{"after": {"a": 9, "b": 9}, "key":, "updated": "1586020760120222000.0000000000"}`,
			false, "", "", 0, 0,
		},
		{
			`{"after": {"a": 9, "b": 9}, "key": [9], "updated": "0.0000000000"}`,
			false, "", "", 0, 0,
		},
		{
			`{"after": {"a": 9, "b": 9}, "updated": "1586020760120222000.0000000000"}`,
			false, "", "", 0, 0,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d - %s", i, test.testcase), func(t *testing.T) {
			actual, actualErr := parseLine([]byte(test.testcase))
			if test.expectedPass && !a.NoError(actualErr) {
				return
			}
			if !test.expectedPass {
				return
			}
			a.Equal(test.expectedNanos, actual.nanos)
			a.Equal(test.expectedLogical, actual.logical)
			a.Equal(test.expectedKey, actual.key)
			a.Equal(test.expectedAfter, actual.after)
		})
	}
}

func TestWriteToSinkTable(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Give the from table a few rows
	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	if count, err := tableFrom.getTableRowCount(ctx); a.NoError(err) {
		a.Equal(10, count)
	} else {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	sink := sinks.FindSink(endpointTest, tableFrom.name)
	if !a.NotNil(sink) {
		return
	}

	// Make sure there are no rows in the table yet.
	if rowCount, err := getRowCount(ctx, db, sink.sinkTableFullName); a.NoError(err) {
		a.Equal(0, rowCount)
	} else {
		return
	}

	// Write 100 rows to the table.
	var lines []Line
	for i := 0; i < 100; i++ {
		lines = append(lines, Line{
			nanos:   int64(i),
			logical: i,
			key:     fmt.Sprintf("[%d]", i),
			after:   fmt.Sprintf(`{"a": %d`, i),
		})
	}

	if err := WriteToSinkTable(ctx, db, sink.sinkTableFullName, lines); !a.NoError(err) {
		return
	}

	// Check to see if there are indeed 100 rows in the table.
	if rowCount, err := getRowCount(ctx, db, sink.sinkTableFullName); a.NoError(err) {
		a.Equal(100, rowCount)
	}
}

func TestFindAllRowsToUpdate(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	// Insert 100 rows into the table.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	var lines []Line
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			lines = append(lines, Line{
				nanos:   int64(i),
				logical: j,
				after:   fmt.Sprintf("{a=%d,b=%d}", i, j),
				key:     fmt.Sprintf("[%d]", i),
			})
		}
	}
	if err := WriteToSinkTable(ctx, db, sink.sinkTableFullName, lines); !a.NoError(err) {
		return
	}

	// Now find those rows from the start.
	for i := 0; i < 10; i++ {
		prev := ResolvedLine{
			endpoint: "test",
			nanos:    0,
			logical:  0,
		}
		next := ResolvedLine{
			endpoint: "test",
			nanos:    int64(i),
			logical:  i,
		}
		lines, err := findAllRowsToUpdateDB(ctx, db, sink.sinkTableFullName, prev, next)
		if a.NoError(err) {
			a.Len(lines, i*11)
		}
	}

	// And again but from the previous.
	for i := 1; i < 10; i++ {
		prev := ResolvedLine{
			endpoint: "test",
			nanos:    int64(i - 1),
			logical:  i - 1,
		}
		next := ResolvedLine{
			endpoint: "test",
			nanos:    int64(i),
			logical:  i,
		}
		lines, err := findAllRowsToUpdateDB(ctx, db, sink.sinkTableFullName, prev, next)
		if a.NoError(err) {
			a.Len(lines, 11)
		}
	}
}
