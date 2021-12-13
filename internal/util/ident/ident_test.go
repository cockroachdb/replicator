// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ident

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdent(t *testing.T) {
	a := assert.New(t)

	a.True(New("").Empty())

	id := New("table")
	a.Equal("table", id.Raw())
	a.Equal(`"table"`, id.String())
	a.False(id.Empty())

	a.Equal(id, New("table"))

	a.Equal(`"foo!bar"`, New("foo!bar").String())
}

func TestQualified(t *testing.T) {
	a := assert.New(t)

	id := NewTable(New("database"), New("schema"), New("table"))
	a.Equal(`"database"."schema"."table"`, id.String())
}

func TestRelative(t *testing.T) {
	foo := New("foo")

	tcs := []struct {
		table       string
		expected    Table
		expectError bool
	}{
		{
			table:       "",
			expectError: true,
		},
		{
			table:    "foo",
			expected: NewTable(StagingDb, Public, foo),
		},
		{
			table:    "other.foo",
			expected: NewTable(StagingDb, Public, foo),
		},
		{
			table:    "other.schema.foo",
			expected: NewTable(StagingDb, New("schema"), foo),
		},
		{
			table:       "other.wat.schema.foo",
			expectError: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.table, func(t *testing.T) {
			a := assert.New(t)
			parsed, err := Relative(StagingDb, tc.table)
			if tc.expectError {
				a.Error(err)
				return
			}
			a.Equal(tc.expected, parsed)
		})
	}
}
