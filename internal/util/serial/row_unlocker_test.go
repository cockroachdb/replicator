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

	"github.com/stretchr/testify/assert"
)

func TestRowUnlocker(t *testing.T) {
	tcs := []struct {
		name        string
		expectError bool
		forced      error
		row         *fakeRow
	}{
		{name: "happy path", row: &fakeRow{}},
		{name: "bad scan", expectError: true, row: &fakeRow{errExpected}},
		{name: "forced error", expectError: true, forced: errExpected},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)

			var b fakeUnlockable
			u := &rowUnlocker{err: tc.forced, r: tc.row, u: &b}
			if tc.expectError {
				a.ErrorIs(u.Scan(), errExpected)
			} else {
				a.NoError(u.Scan())
			}
			a.True(b.Unlocked())

			// Ensure that a redundant call to unlock() is ok
			u.unlock()
		})
	}
}
