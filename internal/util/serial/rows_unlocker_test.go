// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serial

import (
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

func TestRowsUnlocker(t *testing.T) {
	tcs := []struct {
		name        string
		expectError bool
		forced      error
		justClose   bool
		rows        *fakeRows
	}{
		{name: "happy path", rows: &fakeRows{rowCount: 10}},
		{name: "empty rows", rows: &fakeRows{rowCount: 0}},
		{name: "rows error", expectError: true, rows: &fakeRows{err: errExpected, rowCount: 1}},
		{name: "just close", justClose: true},
	}

	// Drain the Rows object, using either the Scan or the Values
	// methods.
	drainCall := 0
	drain := func(r pgx.Rows) error {
		for r.Next() {
			drainCall++
			var err error
			if drainCall%2 == 0 {
				err = r.Scan()
			} else {
				_, err = r.Values()
			}
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)

			var b fakeUnlockable
			rows := &rowsUnlocker{r: tc.rows, u: &b}

			if tc.justClose {
				rows.Close()
			} else {
				err := drain(rows)
				if tc.expectError {
					a.ErrorIs(err, errExpected)
					a.ErrorIs(rows.Err(), errExpected)
				} else {
					a.NoError(err)
					a.NoError(rows.Err())
				}
			}

			a.True(b.Unlocked())

			// Verify that explicit, if unnecessary, close is ok.
			rows.Close()
		})
	}
}
