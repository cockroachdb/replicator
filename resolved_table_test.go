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
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

func (rl ResolvedLine) writeUpdatedDB(ctx context.Context, db *pgxpool.Pool) error {
	return Retry(ctx, func(ctx context.Context) error {
		return rl.writeUpdated(ctx, db)
	})
}

func getPreviousResolvedDB(ctx context.Context, db *pgxpool.Pool, endpoint string) (ResolvedLine, error) {
	var resolvedLine ResolvedLine
	if err := Retry(ctx, func(ctx context.Context) error {
		var err error
		resolvedLine, err = getPreviousResolved(ctx, db, endpoint)
		return err
	}); err != nil {
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
			a := assert.New(t)
			actual, actualErr := parseResolvedLine([]byte(test.testcase), "endpoint.sql")
			if test.expectedPass && !a.NoError(actualErr) {
				return
			}
			if !test.expectedPass {
				return
			}
			a.Equal(test.expectedNanos, actual.nanos, "nanos")
			a.Equal(test.expectedLogical, actual.logical, "logical")
			a.Equal(test.expectedEndpoint, actual.endpoint, "endpoint")
		})
	}
}

func TestResolvedTable(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, _, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	if !a.NoError(CreateResolvedTable(ctx, db)) {
		return
	}

	checkResolved := func(y ResolvedLine, z ResolvedLine) bool {
		return a.Equal(y.endpoint, z.endpoint, "endpoint") &&
			a.Equal(y.nanos, z.nanos, "nanos") &&
			a.Equal(y.logical, z.logical, "logical")
	}

	// Make sure there are no rows in the table yet.
	if rowCount, err := getRowCount(ctx, db, resolvedFullTableName()); !a.NoError(err) ||
		!a.Equal(0, rowCount) {
		return
	}

	// Find no previous value for endpoint "one".
	if one, err := getPreviousResolvedDB(ctx, db, "one"); !a.NoError(err) ||
		!checkResolved(ResolvedLine{endpoint: "one"}, one) {
		return
	}

	// Push 10 updates rows to the resolved table and check each one.
	for i := 0; i < 10; i++ {
		newOne := ResolvedLine{
			endpoint: "one",
			nanos:    int64(i),
			logical:  i,
		}
		if err := newOne.writeUpdatedDB(ctx, db); !a.NoError(err) {
			return
		}
		if previousOne, err := getPreviousResolvedDB(ctx, db, "one"); !a.NoError(err) ||
			!checkResolved(newOne, previousOne) {
			return
		}
	}

	// Now do the same for a second endpoint.
	if two, err := getPreviousResolvedDB(ctx, db, "two"); !a.NoError(err) ||
		!checkResolved(ResolvedLine{endpoint: "two"}, two) {
		return
	}

	// Push 10 updates rows to the resolved table and check each one.
	for i := 0; i < 10; i++ {
		newOne := ResolvedLine{
			endpoint: "two",
			nanos:    int64(i),
			logical:  i,
		}
		if err := newOne.writeUpdatedDB(ctx, db); !a.NoError(err) {
			return
		}
		if previousOne, err := getPreviousResolvedDB(ctx, db, "two"); !a.NoError(err) ||
			!checkResolved(newOne, previousOne) {
			return
		}
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

		if err := newResolved.writeUpdatedDB(ctx, db); !a.NoError(err) {
			return
		}
		previousResolved, err := getPreviousResolvedDB(ctx, db, newResolved.endpoint)
		if !a.NoError(err) || !checkResolved(newResolved, previousResolved) {
			return
		}
	}

	// Finally, check to make sure that there are only 2 lines in the resolved
	// table.
	rowCount, err := getRowCount(ctx, db, resolvedFullTableName())
	a.Equal(2, rowCount, "rowCount")
	a.NoError(err)
}
