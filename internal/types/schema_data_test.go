// Copyright 2024 The Cockroach Authors
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

package types

import (
	"slices"
	"testing"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestSetComponents(t *testing.T) {
	a := assert.New(t)

	const bigGroupSize = 10
	const numComps = 4
	tcs := []struct {
		name      string
		order     int // Ensure stable, schema-wide ordering
		groupSize int
		parents   []string
	}{
		{
			"parent",
			1,
			bigGroupSize,
			nil,
		},
		{
			"another_table",
			0,
			1,
			nil,
		},
		{
			"unreferenced",
			2,
			1,
			nil,
		},
		{
			"child",
			3,
			bigGroupSize,
			[]string{"parent"},
		},
		{
			"child_2",
			4,
			bigGroupSize,
			[]string{"parent"},
		},
		{
			"grandchild",
			7,
			bigGroupSize,
			[]string{"child"},
		},
		{
			"grandchild_2",
			8,
			bigGroupSize,
			[]string{"child_2"},
		},
		{
			"grandchild_multi",
			9,
			bigGroupSize,
			[]string{"child", "child_2"},
		},
		{
			"three",
			10,
			bigGroupSize,
			[]string{"parent", "grandchild"},
		},
		{
			"four",
			11,
			bigGroupSize,
			[]string{"parent", "three"},
		},
		{
			"five",
			12,
			bigGroupSize,
			[]string{"parent", "four"},
		},
		{
			"six",
			13,
			bigGroupSize,
			[]string{"parent", "five"},
		},
		// Verify that a self-referential table returns reasonable values.
		{
			"self",
			5,
			2,
			[]string{"self"},
		},
		{
			"self_child",
			6,
			2,
			[]string{"self"},
		},
	}

	refs := &ident.TableMap[[]ident.Table]{}
	schema := ident.MustSchema(ident.New("schema"))
	tables := make([]ident.Table, len(tcs))

	for idx, tcs := range tcs {
		table := ident.NewTable(schema, ident.New(tcs.name))
		tables[idx] = table

		// Ensure stub entry for tables.
		refs.Put(table, refs.GetZero(table))

		for _, parentRaw := range tcs.parents {
			parent := ident.NewTable(schema, ident.New(parentRaw))
			refs.Put(parent, append(refs.GetZero(parent), table))
		}
	}

	sd := &SchemaData{}
	a.NoError(sd.SetComponents(refs))
	a.Len(sd.Components, numComps)
	a.Len(sd.Entire.Order, len(tcs))
	a.Equal(len(tcs), sd.TableComponents.Len())

	globalOrder := func(table ident.Table) int {
		return slices.IndexFunc(sd.Entire.Order, func(elt ident.Table) bool {
			return ident.Equal(elt, table)
		})
	}

	for idx, tcs := range tcs {
		table := tables[idx]
		comp, ok := sd.TableComponents.Get(table)
		a.Truef(ok, "%s", table)
		a.NotNilf(comp, "%s", table)
		a.Lenf(comp.Order, tcs.groupSize, "%s", table)

		// Verify global ordering is correct.
		a.Equalf(tcs.order, globalOrder(table), "%s", table)

	}

	for _, comp := range sd.Components {
		// Verify order within the component aligns with global order.
		a.True(slices.IsSortedFunc(comp.Order, func(a, b ident.Table) int {
			aIdx := globalOrder(a)
			bIdx := globalOrder(b)
			return aIdx - bIdx
		}))

		a.Equal(len(comp.Order), len(comp.ReverseOrder))
	}

	// Add a reference cycle and verify error.
	cycle1 := ident.NewTable(schema, ident.New("cycle1"))
	cycle2 := ident.NewTable(schema, ident.New("cycle2"))
	refs.Put(cycle1, []ident.Table{cycle2})
	refs.Put(cycle2, []ident.Table{cycle1})
	a.ErrorContains(sd.SetComponents(refs), "cycle detected")
}
