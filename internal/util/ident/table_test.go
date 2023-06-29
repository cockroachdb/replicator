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

package ident

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		a := assert.New(t)
		var tbl Table
		a.Empty(tbl.Idents(nil))
		a.Equal("", tbl.String())
		a.Equal("", tbl.Raw())
		a.True(tbl.Empty())
		a.Equal(Schema{}, tbl.Schema())
		a.Equal(Ident{}, tbl.Table())

		checkTableJSON(t, tbl)
		checkTableText(t, tbl)
	})

	t.Run("empty", func(t *testing.T) {
		a := assert.New(t)
		tbl := NewTable(Schema{}, Ident{})
		a.Empty(tbl.Idents(nil))
		a.Equal("", tbl.String())
		a.Equal("", tbl.Raw())
		a.True(tbl.Empty())
		a.Equal(Schema{}, tbl.Schema())
		a.Equal(Ident{}, tbl.Table())

		checkTableJSON(t, tbl)
		checkTableText(t, tbl)
	})

	t.Run("one", func(t *testing.T) {
		a := assert.New(t)
		tbl := NewTable(Schema{}, New("table"))
		a.Len(tbl.Idents(nil), 1)
		a.Equal(`"table"`, tbl.String())
		a.Equal(`table`, tbl.Raw())
		a.False(tbl.Empty())
		a.Equal(Schema{}, tbl.Schema())
		a.Equal(New("table"), tbl.Table())

		checkTableJSON(t, tbl)
		checkTableText(t, tbl)
	})

	t.Run("two", func(t *testing.T) {
		a := assert.New(t)
		tbl := NewTable(MustSchema(New("db")), New("table"))
		a.Len(tbl.Idents(nil), 2)
		a.Equal(`"db"."table"`, tbl.String())
		a.Equal(`db.table`, tbl.Raw())
		a.False(tbl.Empty())
		a.Equal(MustSchema(New("db")), tbl.Schema())
		a.Equal(New("table"), tbl.Table())

		checkTableJSON(t, tbl)
		checkTableText(t, tbl)
	})

	t.Run("three", func(t *testing.T) {
		a := assert.New(t)
		tbl := NewTable(MustSchema(New("db"), Public), New("table"))
		a.Len(tbl.Idents(nil), 3)
		a.Equal(`"db"."public"."table"`, tbl.String())
		a.Equal(`db.public.table`, tbl.Raw())
		a.False(tbl.Empty())
		a.Equal(MustSchema(New("db"), Public), tbl.Schema())
		a.Equal(New("table"), tbl.Table())

		checkTableJSON(t, tbl)
		checkTableText(t, tbl)
	})
}

func TestQualified(t *testing.T) {
	a := assert.New(t)

	id := NewTable(MustSchema(New("database"), New("schema")), New("table"))
	a.Equal(`"database"."schema"."table"`, id.String())
}

func TestTableJson(t *testing.T) {
	tcs := []Table{
		{},
		NewTable(Schema{}, New("table")),
		NewTable(MustSchema(New("db")), New("table")),
		NewTable(MustSchema(New("db"), New("schema")), New("table")),
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)

			data, err := json.Marshal(tc)
			a.NoError(err)

			var parsed Table
			a.NoError(json.Unmarshal(data, &parsed))
			a.Equal(tc, parsed)
		})
	}
}

func checkTableJSON(t *testing.T, tbl Table) {
	t.Helper()
	a := assert.New(t)

	data, err := json.Marshal(tbl)
	a.NoError(err)

	var decoded Table
	a.NoError(json.Unmarshal(data, &decoded))
	a.Equal(tbl, decoded)
}

func checkTableText(t *testing.T, tbl Table) {
	t.Helper()
	a := assert.New(t)

	data, err := tbl.MarshalText()
	a.NoError(err)

	var decoded Table
	a.NoError(decoded.UnmarshalText(data))
	a.Equal(tbl, decoded)
}
