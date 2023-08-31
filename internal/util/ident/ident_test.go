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
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestIdent(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		a := assert.New(t)
		var id Ident
		a.Empty(id.Idents(nil))
		a.Equal(`""`, id.String())
		a.Equal("", id.Raw())
		a.True(id.Empty())
		a.Equal(id, id.Canonical())

		first, remainder := id.Split()
		a.True(first.Empty())
		a.True(remainder.Empty())
	})

	t.Run("empty", func(t *testing.T) {
		a := assert.New(t)
		id := New("")
		a.Same(id.atom, id.Idents(nil)[0].atom)
		a.Equal(`""`, id.String())
		a.Equal("", id.Raw())
		a.True(id.Empty())
		a.Equal(id, id.Canonical())

		first, remainder := id.Split()
		a.True(first.Empty())
		a.True(remainder.Empty())
	})

	t.Run("typical", func(t *testing.T) {
		a := assert.New(t)
		id := New("table")
		a.Same(id.atom, id.Idents(nil)[0].atom)
		a.Equal("table", id.Raw())
		a.Equal(`"table"`, id.String())
		a.False(id.Empty())
		a.Same(id.atom, id.lowered)

		first, remainder := id.Split()
		a.Equal(id, first)
		a.True(remainder.Empty())
	})

	t.Run("canonical", func(t *testing.T) {
		a := assert.New(t)

		id := New("table")
		id2 := New("table")
		id3 := New("other")
		id4 := New("TABLE")

		a.Same(id.atom, id2.atom)
		a.NotSame(id.atom, id3.atom)

		a.NotSame(id.atom, id4.atom)
		a.Same(id.atom, id4.lowered)
		a.Equal(id, id4.Canonical())
	})
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

	t.Run("zero", func(t *testing.T) {
		a := assert.New(t)
		buf, err := Ident{}.MarshalJSON()
		a.NoError(err)
		a.Equal([]byte(`""`), buf)
	})

	for _, tc := range tcs {
		t.Run(tc.raw+"-json", func(t *testing.T) {
			a := assert.New(t)

			id := New(tc.raw)
			data, err := json.Marshal(id)
			a.NoError(err)

			var id2 Ident
			a.NoError(json.Unmarshal(data, &id2))
			a.Equal(id, id2)
			a.Same(id.atom, id2.atom)
		})
		t.Run(tc.raw+"-text", func(t *testing.T) {
			a := assert.New(t)

			id := New(tc.raw)
			data, err := id.MarshalText()
			a.NoError(err)

			var id2 Ident
			a.NoError(id2.UnmarshalText(data))
			a.Equal(id, id2)
			a.Same(id.atom, id2.atom)
		})
	}
}

func TestIdentSize(t *testing.T) {
	a := assert.New(t)

	ptrSize := unsafe.Sizeof(uintptr(0))
	a.Equal(expectedIdentWords, int(unsafe.Sizeof(Ident{})/ptrSize))
	a.Equal(expectedIdentWords, int(unsafe.Sizeof(Schema{})/ptrSize))
	a.Equal(expectedIdentWords, int(unsafe.Sizeof(Table{})/ptrSize))
	// Plus one for the array field.
	a.Equal(expectedIdentWords+1, int(unsafe.Sizeof(UDT{})/ptrSize))
}
