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

package merge

import (
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestEmpty(t *testing.T) {
	a := assert.New(t)

	colData := []types.ColData{
		{Name: ident.New("col0"), Primary: true},
		{Name: ident.New("col1"), Primary: true},
		{Name: ident.New("col2")},
	}

	b := NewBagOf(colData, ident.MapOf[ident.Ident](
		ident.New("col1_source"), colData[1].Name,
	))

	// Expect entries to be pre-created.
	a.Equal(len(colData), b.Mapped.Len())
	// Those entries should not have their valid flag set.
	a.NoError(b.Mapped.Range(func(_ ident.Ident, entry *Entry) error {
		a.False(entry.Valid)
		return nil
	}))
	a.Equal(0, b.Len())

	// Nothing has been set, so entries should be empty.
	a.Empty(b.Entries())
	for _, col := range colData {
		a.Zero(b.GetZero(col.Name))

		found, ok := b.Get(col.Name)
		a.Zero(found)
		a.False(ok)

		original, found, ok := b.Match(col.Name)
		a.Zero(original)
		a.Zero(found)
		a.False(ok)
	}
	// CopyInto shouldn't do anything.
	var temp ident.Map[any]
	b.CopyInto(&temp)
	a.Equal(0, temp.Len())
	// Should serialize to an empty object literal.
	if data, err := b.MarshalJSON(); a.NoError(err) {
		a.Equal([]byte("{}"), data)
	}
	a.NoError(b.Range(func(_ ident.Ident, _ any) error {
		return errors.New("should be empty")
	}))

	a.NoError(ValidateNoUnmappedColumns(b))
	a.ErrorContains(ValidatePK(b), "col0")
}

func TestBag(t *testing.T) {
	a := assert.New(t)

	colData := []types.ColData{
		{Name: ident.New("col0"), Primary: true},
		{Name: ident.New("col1")},
		{Name: ident.New("col2")},
	}
	renamed1 := ident.New("col1_source")
	unmapped := ident.New("unmapped")
	expected := map[string]any{
		colData[0].Name.Raw(): "0",
		renamed1.Raw():        "1",
		colData[2].Name.Raw(): "2",
		unmapped.Raw():        "unmapped",
	}

	b := NewBagOf(colData, ident.MapOf[ident.Ident](
		ident.New("col1_source"), colData[1].Name,
	))
	// Put()
	for k, v := range expected {
		b.Put(ident.New(k), v)
	}

	// Len() and Entries()
	a.Equal(len(expected), b.Len())
	a.Len(b.Entries(), len(expected))

	// Check internal state.
	a.Equal(len(colData), b.Mapped.Len())
	a.Equal(len(expected)-len(colData), b.Unmapped.Len())

	// CopyInto()
	var temp ident.Map[any]
	b.CopyInto(&temp)
	a.Equal(len(expected), temp.Len())

	// Range()
	ct := 0
	a.NoError(b.Range(func(ident.Ident, any) error {
		ct++
		return nil
	}))
	a.Equal(len(expected), ct)

	// Getters and Match()
	a.NoError(b.Range(func(k ident.Ident, v any) error {
		a.Equal(v, b.GetZero(k))

		found, ok := b.Get(k)
		a.Equal(v, found)
		a.True(ok)

		original, found, ok := b.Match(k)
		a.Equal(k, original)
		a.Equal(v, found)
		a.True(ok)
		return nil
	}))

	data, err := b.MarshalJSON()
	if a.NoError(err) {
		next := NewBagFrom(b)
		a.Equal(0, next.Len())
		if a.NoError(next.UnmarshalJSON(data)) {
			a.Equal(4, next.Len())
		}
	}

	a.ErrorContains(ValidateNoUnmappedColumns(b), "unmapped")
	a.NoError(ValidatePK(b))

	a.NoError(b.Range(func(k ident.Ident, _ any) error {
		b.Delete(k)
		return nil
	}))
	a.Equal(0, b.Len())
}
