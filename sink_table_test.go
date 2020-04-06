package main

import (
	"database/sql"
	"fmt"
	"math"
	"testing"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

// findAllRowsToUpdateDB is a wrapper around FindAllRowsToUpdate that handles
// the transaction for testing.
func findAllRowsToUpdateDB(
	db *sql.DB, sinkTableFullName string, prev ResolvedLine, next ResolvedLine,
) ([]Line, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	lines, err := FindAllRowsToUpdate(tx, sinkTableFullName, prev, next)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
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
			if test.expectedPass == (actualErr != nil) {
				t.Errorf("Expected %v, got %s", test.expectedPass, actualErr)
			}
			if !test.expectedPass {
				return
			}
			if test.expectedNanos != actual.nanos {
				t.Errorf("Expected %d nanos, got %d nanos", test.expectedNanos, actual.nanos)
			}
			if test.expectedLogical != actual.logical {
				t.Errorf("Expected %d logical, got %d logical", test.expectedLogical, actual.logical)
			}
			if test.expectedKey != actual.key {
				t.Errorf("Expected %s key, got %s key", test.expectedKey, actual.key)
			}
			if test.expectedAfter != actual.after {
				t.Errorf("Expected %s after, got %s after", test.expectedAfter, actual.after)
			}
		})
	}
}

func TestWriteToSinkTable(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Drop the previous _cdc_sink db
	if err := DropSinkDB(db); err != nil {
		t.Fatal(err)
	}

	// Create a new _cdc_sink db
	if err := CreateSinkDB(db); err != nil {
		t.Fatal(err)
	}

	// Create the table to import from
	tableFrom := createTestTable(t, db, dbName)

	// Create the table to receive into
	tableTo := createTestTable(t, db, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(t, 10)
	if count := tableFrom.getTableRowCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks := CreateSinks()
	if err := sinks.AddSink(db, tableFrom.name, tableTo.dbName, tableTo.name); err != nil {
		t.Fatal(err)
	}

	sink := sinks.FindSink(tableFrom.name)
	if sink == nil {
		t.Fatalf("Expected sink, found none")
	}

	// Make sure there are no rows in the table yet.
	if rowCount := getRowCount(t, db, sink.sinkTableFullName); rowCount != 0 {
		t.Fatalf("Expected 0 rows, got %d", rowCount)
	}

	// Write 100 rows to the table.
	for i := 0; i < 100; i++ {
		line := Line{
			nanos:   int64(i),
			logical: i,
			key:     fmt.Sprintf("[%d]", i),
			after:   fmt.Sprintf(`{"a": %d`, i),
		}
		line.WriteToSinkTable(db, sink.sinkTableFullName)
	}

	// Check to see if there are indeed 100 rows in the table.
	if rowCount := getRowCount(t, db, sink.sinkTableFullName); rowCount != 100 {
		t.Fatalf("Expected 0 rows, got %d", rowCount)
	}
}

func TestFindAllRowsToUpdate(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Drop the previous _cdc_sink db
	if err := DropSinkDB(db); err != nil {
		t.Fatal(err)
	}

	// Create a new _cdc_sink db
	if err := CreateSinkDB(db); err != nil {
		t.Fatal(err)
	}

	// Create the table to import from
	tableFrom := createTestTable(t, db, dbName)

	// Create the table to receive into
	tableTo := createTestTable(t, db, dbName)

	// Create the sinks and sink
	sinks := CreateSinks()
	if err := sinks.AddSink(db, tableFrom.name, tableTo.dbName, tableTo.name); err != nil {
		t.Fatal(err)
	}

	// Insert 100 rows into the table.
	sink := sinks.FindSink(tableFrom.name)
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			line := Line{
				nanos:   int64(i),
				logical: j,
				after:   fmt.Sprintf("{a=%d,b=%d}", i, j),
				key:     fmt.Sprintf("[%d]", i),
			}
			if err := line.WriteToSinkTable(db, sink.sinkTableFullName); err != nil {
				t.Fatal(err)
			}
		}
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
		lines, err := findAllRowsToUpdateDB(db, sink.sinkTableFullName, prev, next)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) != i*11 {
			t.Errorf("expected %d lines, got %d", i*11, len(lines))
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
		lines, err := findAllRowsToUpdateDB(db, sink.sinkTableFullName, prev, next)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) != 11 {
			t.Errorf("expected %d lines, got %d", 11, len(lines))
		}
	}
}
