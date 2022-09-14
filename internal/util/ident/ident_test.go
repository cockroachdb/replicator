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
)

func TestFlagVar(t *testing.T) {
	a := assert.New(t)

	var id Ident
	a.True(id.IsEmpty())

	value := NewValue("default", &id)
	a.Equal(New("default"), id)

	a.NoError(value.Set("different"))
	a.Equal(New("different"), id)
}

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

func TestIdentJson(t *testing.T) {
	tcs := []struct {
		raw string
	}{
		{""},
		{"foo"},
		{`"foo"`},
		{"null"},
	}

	for _, tc := range tcs {
		t.Run(tc.raw, func(t *testing.T) {
			a := assert.New(t)

			id := New(tc.raw)
			data, err := json.Marshal(id)
			a.NoError(err)

			var id2 Ident
			a.NoError(json.Unmarshal(data, &id2))
			a.Equal(id, id2)
		})
	}
}

func TestQualified(t *testing.T) {
	a := assert.New(t)

	id := NewTable(New("database"), New("schema"), New("table"))
	a.Equal(`"database"."schema"."table"`, id.String())
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

func TestTableJson(t *testing.T) {
	a := assert.New(t)

	id := NewTable(New("db"), New("schema"), New("table"))
	data, err := json.Marshal(id)
	a.NoError(err)

	var id2 Table
	a.NoError(json.Unmarshal(data, &id2))
	a.Equal(id, id2)
}

func TestUDTJson(t *testing.T) {
	a := assert.New(t)

	id := NewUDT(New("db"), New("schema"), New("my_enum"))
	data, err := json.Marshal(id)
	a.NoError(err)

	var id2 UDT
	a.NoError(json.Unmarshal(data, &id2))
	a.Equal(id, id2)
}
