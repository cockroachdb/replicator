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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchema(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		a := assert.New(t)
		var s Schema
		a.Empty(s.Idents(nil))
		a.Equal("", s.String())
		a.Equal("", s.Raw())
		a.True(s.Empty())
		a.Equal(s, s.Schema())

		// Zero schema contains no tables.
		a.False(s.Contains(NewTable(s, New("foo"))))

		first, remainder := s.Split()
		a.True(first.Empty())
		a.True(remainder.Empty())

		checkSchemaJSON(t, s)
	})

	t.Run("empty", func(t *testing.T) {
		a := assert.New(t)
		s, err := NewSchema()
		a.NoError(err)
		a.Empty(s.Idents(nil))
		a.Equal("", s.String())
		a.Equal("", s.Raw())
		a.True(s.Empty())
		a.Same(s.array, MustSchema().array)
		a.Equal(s, s.Schema())

		// Empty schema contains no tables.
		a.False(s.Contains(NewTable(s, New("foo"))))

		first, remainder := s.Split()
		a.True(first.Empty())
		a.True(remainder.Empty())

		checkSchemaJSON(t, s)
	})

	t.Run("one", func(t *testing.T) {
		a := assert.New(t)
		s, err := NewSchema(New("foo"))
		a.NoError(err)
		a.Len(s.Idents(nil), 1)
		a.Equal(`"foo"`, s.String())
		a.Equal("foo", s.Raw())
		a.False(s.Empty())
		a.Same(s.array, MustSchema(New("foo")).array)
		a.True(s.Contains(NewTable(s, New("foo"))))
		a.Equal(s, s.Schema())

		first, remainder := s.Split()
		a.Equal(New("foo"), first)
		a.True(remainder.Empty())

		checkSchemaJSON(t, s)
	})

	t.Run("two", func(t *testing.T) {
		a := assert.New(t)
		s, err := NewSchema(New("foo"), New("bar"))
		a.NoError(err)
		a.Len(s.Idents(nil), 2)
		a.Equal(`"foo"."bar"`, s.String())
		a.Equal("foo.bar", s.Raw())
		a.False(s.Empty())
		a.Same(s.array, MustSchema(New("foo"), New("bar")).array)
		a.True(s.Contains(NewTable(s, New("foo"))))
		a.Equal(s, s.Schema())

		first, remainder := s.Split()
		a.Equal(New("foo"), first)
		a.False(remainder.Empty())

		second, remainder := remainder.Split()
		a.Equal(New("bar"), second)
		a.True(remainder.Empty())

		checkSchemaJSON(t, s)
	})

	t.Run("three", func(t *testing.T) {
		a := assert.New(t)
		_, err := NewSchema(New("foo"), New("bar"), New("boom"))
		a.ErrorContains(err, "number of parts 3 exceeds maximum 2")

		var sch Schema
		a.ErrorContains(sch.UnmarshalJSON([]byte(`["one","two","three"]`)),
			"expecting at most 2 parts, had 3")
	})
}

func checkSchemaJSON(t *testing.T, sch Schema) {
	t.Helper()
	a := assert.New(t)

	data, err := json.Marshal(sch)
	a.NoError(err)

	var decoded Schema
	a.NoError(json.Unmarshal(data, &decoded))
	a.Equal(sch, decoded)
}
