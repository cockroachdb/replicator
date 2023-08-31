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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseIdent(t *testing.T) {
	tcs := []struct {
		input     string
		ident     Ident
		remainder string
		err       string
	}{
		{
			input: "",
			err:   "cannot parse empty string",
		},
		{
			input: `hello world`,
			ident: New("hello world"),
		},
		{
			input: `hello🪳world`,
			ident: New("hello🪳world"),
		},
		{
			input:     `hello.world`,
			ident:     New("hello"),
			remainder: ".world",
		},
		{
			input:     `"hello world"`,
			ident:     New("hello world"),
			remainder: "",
		},
		{
			input: `"hello world`,
			ident: New("hello world"),
			err:   "did not find trailing quote",
		},
		{
			input:     `"hello🪳world".more`,
			ident:     New("hello🪳world"),
			remainder: ".more",
		},
		{
			input:     `"hello""world"`,
			ident:     New(`hello"world`),
			remainder: "",
		},
		{
			input:     `"hello""world".more`,
			ident:     New(`hello"world`),
			remainder: ".more",
		},
		{
			input:     `"""hello""world"""`,
			ident:     New(`"hello"world"`),
			remainder: "",
		},
		{
			input:     `"""hello""""world"""`,
			ident:     New(`"hello""world"`),
			remainder: "",
		},
		{
			input:     `"""""hello""""world"""""`,
			ident:     New(`""hello""world""`),
			remainder: "",
		},
		{
			input: "is \uFFFD error",
			err:   "malformed UTF8 input",
		},
		{
			input: "\"is \uFFFD error\"",
			err:   "malformed UTF8 input",
		},
		{
			input: `baz[]`,
			ident: New(`baz[]`),
		},
		{
			input:     `"baz"[]`,
			ident:     New(`baz`),
			remainder: "[]",
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			ident, remainder, err := ParseIdent(tc.input)
			if tc.err == "" {
				a.Equal(tc.ident, ident)
				a.Equal(tc.remainder, remainder)
				a.NoError(err)

				reparsed, remainder, err := ParseIdent(ident.String())
				a.NoError(err)
				a.Equal(ident, reparsed)
				a.Empty(remainder)
			} else {
				a.ErrorContains(err, tc.err)
			}
		})
	}
}

func TestParseSchema(t *testing.T) {
	tcs := []struct {
		input    string
		expected Schema
	}{
		{"", MustSchema()},
		{"foo", MustSchema(New("foo"))},
		{"foo.bar", MustSchema(New("foo"), New("bar"))},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			s, err := ParseSchema(tc.input)
			a.NoError(err)
			a.Equal(tc.expected, s)
		})
	}
}

func TestParseTable(t *testing.T) {
	base := New("base")
	sch := New("schema")
	relTo := MustSchema(base, sch)
	tcs := []struct {
		input string
		table Table
		qual  Qualification
		err   string
	}{
		{
			input: "table",
			table: NewTable(MustSchema(base, sch), New("table")),
			qual:  TableOnly,
		},
		{
			input: "s2.table",
			table: NewTable(MustSchema(base, New("s2")), New("table")),
			qual:  PartialSchema,
		},
		{
			input: "db.s2.table",
			table: NewTable(MustSchema(New("db"), New("s2")), New("table")),
			qual:  FullyQualified,
		},
		{
			input: `db.s2."Foo.Bar.Baz"`,
			table: NewTable(MustSchema(New("db"), New("s2")), New("Foo.Bar.Baz")),
			qual:  FullyQualified,
		},
		{
			input: "db..table",
			err:   "cannot start with separator",
		},
		{
			input: `"db"table`,
			err:   "expecting separator",
		},
		{
			input: "",
			err:   "empty table name",
		},
		{
			input: "this.is.too.long",
			err:   "expecting no more than 2 schema parts, saw 3",
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)

			tbl, qual, err := ParseTableRelative(tc.input, relTo)
			if tc.err == "" {
				a.Equal(tc.table, tbl)
				a.Equal(tc.qual, qual)
				a.NoError(err)

				reparsed, qual, err := ParseTableRelative(tbl.String(), relTo)
				a.Equal(tbl, reparsed)
				a.NoError(err)
				a.Equal(FullyQualified, qual)
			} else {
				a.ErrorContains(err, tc.err)
			}
		})
	}
}
