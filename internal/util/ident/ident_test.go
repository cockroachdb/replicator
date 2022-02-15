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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestIdent(t *testing.T) {
	a := assert.New(t)

	a.True(New("").IsEmpty())

	id := New("table")
	a.Equal("table", id.Raw())
	a.Equal(`"table"`, id.String())
	a.False(id.IsEmpty())

	a.Equal(id, New("table"))

	a.Equal(`"foo!bar"`, New("foo!bar").String())
}

func TestIdentMarshal(t *testing.T) {
	tcs := []struct {
		raw string
	}{
		{""},
		{"foo"},
		{`"foo"`},
		{"null"},
	}

	for _, tc := range tcs {
		t.Run("json "+tc.raw, func(t *testing.T) {
			a := assert.New(t)

			id := New(tc.raw)
			data, err := json.Marshal(id)
			a.NoError(err)

			var id2 Ident
			a.NoError(json.Unmarshal(data, &id2))
			a.Equal(id, id2)
		})
		t.Run("yaml "+tc.raw, func(t *testing.T) {
			a := assert.New(t)

			id := New(tc.raw)
			data, err := yaml.Marshal(id)
			a.NoError(err)

			var id2 Ident
			a.NoError(yaml.Unmarshal(data, &id2))
			a.Equal(id, id2)
		})
	}
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
		qual        Qualification
	}{
		{
			table:       "",
			expectError: true,
		},
		{
			table:    "foo",
			expected: NewTable(StagingDB, Public, foo),
			qual:     TableOnly,
		},
		{
			table:    "other.foo",
			expected: NewTable(StagingDB, Public, foo),
			qual:     TableAndDatabase,
		},
		{
			table:    "other.schema.foo",
			expected: NewTable(StagingDB, New("schema"), foo),
			qual:     FullyQualified,
		},
		{
			table:       "too.many.input.parts",
			expectError: true,
		},
		{
			table:       ".empty.database",
			expectError: true,
		},
		{
			table:       "empty..schema",
			expectError: true,
		},
		{
			table:       "empty.table.",
			expectError: true,
		},
		{
			table:       ".empty_database",
			expectError: true,
		},
		{
			table:       "empty_table.",
			expectError: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.table, func(t *testing.T) {
			a := assert.New(t)
			parsed, qual, err := Relative(tc.expected.Database(), tc.expected.Schema(), tc.table)
			if tc.expectError {
				a.Error(err)
				return
			}
			a.NoError(err)
			a.Equal(tc.expected, parsed)
			a.Equalf(tc.qual, qual, "%s vs %s", tc.qual, qual)
		})
	}
}

func TestSchemaJson(t *testing.T) {
	a := assert.New(t)

	id := NewSchema(New("db"), New("schema"))
	data, err := json.Marshal(id)
	a.NoError(err)

	var id2 Schema
	a.NoError(json.Unmarshal(data, &id2))
	a.Equal(id, id2)
}

func TestSchemaYAML(t *testing.T) {
	a := assert.New(t)

	id := NewSchema(New("db"), New("schema"))
	data, err := yaml.Marshal(id)
	a.NoError(err)
	a.Equal("[db, schema]\n", string(data))

	var id2 Schema
	a.NoError(yaml.Unmarshal(data, &id2))
	a.Equal(id, id2)
}

func TestTableJson(t *testing.T) {
	a := assert.New(t)

	id := NewTable(New("db"), New("schema"), New("table"))
	data, err := json.Marshal(id)
	a.NoError(err)

	var id2 Table
	a.NoError(json.Unmarshal(data, &id2))
	a.Equal(id, id2)
}

func TestTableYAML(t *testing.T) {
	a := assert.New(t)

	id := NewTable(New("db"), New("schema"), New("table"))
	data, err := yaml.Marshal(id)
	a.NoError(err)
	a.Equal("[db, schema, table]\n", string(data))

	var id2 Table
	a.NoError(yaml.Unmarshal(data, &id2))
	a.Equal(id, id2)
}
