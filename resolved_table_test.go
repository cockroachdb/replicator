package main

import (
	"database/sql"
	"fmt"
	"testing"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

func (rl ResolvedLine) writeUpdatedDB(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := rl.writeUpdated(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func getPreviousResolvedDB(db *sql.DB, endpoint string) (ResolvedLine, error) {
	tx, err := db.Begin()
	if err != nil {
		return ResolvedLine{}, err
	}
	resolvedLine, err := getPreviousResolved(tx, endpoint)
	if err != nil {
		return ResolvedLine{}, err
	}
	if err := tx.Commit(); err != nil {
		return ResolvedLine{}, err
	}
	return resolvedLine, nil
}

func TestParseResolvedLine(t *testing.T) {
	tests := []struct {
		testcase         string
		expectedPass     bool
		expectedNanos    int64
		expectedLogical  int
		expectedEndpoint string
	}{
		{
			`{"resolved": "1586020760120222000.0000000000"}`,
			true, 1586020760120222000, 0, "endpoint.sql",
		},
		{
			`{}`,
			false, 0, 0, "",
		},
		{
			`"resolved": "1586020760120222000"}`,
			false, 0, 0, "",
		},
		{
			`{"resolved": "0.0000000000"}`,
			false, 0, 0, "",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d - %s", i, test.testcase), func(t *testing.T) {
			actual, actualErr := parseResolvedLine([]byte(test.testcase), "endpoint.sql")
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
			if test.expectedEndpoint != actual.endpoint {
				t.Errorf("Expected %s endpoint, got %s endpoint", test.expectedEndpoint, actual.endpoint)
			}
		})
	}
}

func TestResolvedTable(t *testing.T) {
	// Create the test db
	db, _, dbClose := getDB(t)
	defer dbClose()

	// Drop the previous _cdc_sink db
	if err := DropSinkDB(db); err != nil {
		t.Fatal(err)
	}

	// Create a new _cdc_sink db
	if err := CreateSinkDB(db); err != nil {
		t.Fatal(err)
	}

	if err := CreateResolvedTable(db); err != nil {
		t.Fatal(err)
	}

	checkResolved := func(e ResolvedLine, a ResolvedLine) {
		if e.endpoint != a.endpoint {
			t.Errorf("Expected endpoint: %s, actual: %s", e.endpoint, a.endpoint)
		}
		if e.nanos != a.nanos {
			t.Errorf("Expected nanos: %d, actual: %d", e.nanos, a.nanos)
		}
		if e.logical != a.logical {
			t.Errorf("Expected logical: %d, logical: %d", e.logical, a.logical)
		}
	}

	// Make sure there are no rows in the table yet.
	if rowCount := getRowCount(t, db, resolvedFullTableName()); rowCount != 0 {
		t.Fatalf("Expected 0 rows, got %d", rowCount)
	}

	// Find no previous value for endpoint "one".
	one, err := getPreviousResolvedDB(db, "one")
	if err != nil {
		t.Fatal(err)
	}
	checkResolved(ResolvedLine{endpoint: "one"}, one)

	// Push 10 updates rows to the resolved table and check each one.
	for i := 0; i < 10; i++ {
		newOne := ResolvedLine{
			endpoint: "one",
			nanos:    int64(i),
			logical:  i,
		}
		if err := newOne.writeUpdatedDB(db); err != nil {
			t.Fatal(err)
		}
		previousOne, err := getPreviousResolvedDB(db, "one")
		if err != nil {
			t.Fatal(err)
		}
		checkResolved(newOne, previousOne)
	}

	// Now do the same for a second endpoint.
	two, err := getPreviousResolvedDB(db, "two")
	if err != nil {
		t.Fatal(err)
	}
	checkResolved(ResolvedLine{endpoint: "two"}, two)

	// Push 10 updates rows to the resolved table and check each one.
	for i := 0; i < 10; i++ {
		newOne := ResolvedLine{
			endpoint: "two",
			nanos:    int64(i),
			logical:  i,
		}
		if err := newOne.writeUpdatedDB(db); err != nil {
			t.Fatal(err)
		}
		previousOne, err := getPreviousResolvedDB(db, "two")
		if err != nil {
			t.Fatal(err)
		}
		checkResolved(newOne, previousOne)
	}

	// Now intersperse the updates.
	for i := 100; i < 120; i++ {
		newResolved := ResolvedLine{
			nanos:   int64(i),
			logical: i,
		}
		if i%2 == 0 {
			newResolved.endpoint = "one"
		} else {
			newResolved.endpoint = "two"
		}

		if err := newResolved.writeUpdatedDB(db); err != nil {
			t.Fatal(err)
		}
		previousResolved, err := getPreviousResolvedDB(db, newResolved.endpoint)
		if err != nil {
			t.Fatal(err)
		}
		checkResolved(newResolved, previousResolved)
	}

	// Finally, check to make sure that there are only 2 lines in the resolved
	// table.
	if rowCount := getRowCount(t, db, resolvedFullTableName()); rowCount != 2 {
		t.Fatalf("Expected 2 rows, got %d", rowCount)
	}
}
