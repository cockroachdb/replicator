// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package serial

import (
	"testing"

	"github.com/jackc/pgx/v5"
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
