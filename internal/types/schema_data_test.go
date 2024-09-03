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
	"github.com/stretchr/testify/require"
)

func TestSetComponents(t *testing.T) {
	r := require.New(t)

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

	sd := &SchemaData{
		Columns: &ident.TableMap[[]ColData]{},
	}
	for idx, tcs := range tcs {
		table := ident.NewTable(schema, ident.New(tcs.name))
		tables[idx] = table

		// Add some fake column data to test serialization below.
		sd.Columns.Put(table, []ColData{
			{Name: ident.New("pk"), Primary: true, Type: "int"},
			{Name: ident.New("val"), Type: "varchar"},
		})

		// Ensure stub entry for tables.
		refs.Put(table, refs.GetZero(table))

		for _, parentRaw := range tcs.parents {
			parent := ident.NewTable(schema, ident.New(parentRaw))
			refs.Put(parent, append(refs.GetZero(parent), table))
		}
	}

	r.NoError(sd.SetDependencies(refs))

	t.Run("setDependencies", func(t *testing.T) {
		r := require.New(t)
		r.Len(sd.Components, numComps)
		r.Len(sd.Entire.Order, len(tcs))
		r.Equal(len(tcs), sd.TableComponents.Len())

		globalOrder := func(table ident.Table) int {
			return slices.IndexFunc(sd.Entire.Order, func(elt ident.Table) bool {
				return ident.Equal(elt, table)
			})
		}

		for idx, tcs := range tcs {
			table := tables[idx]
			comp, ok := sd.TableComponents.Get(table)
			r.Truef(ok, "%s", table)
			r.NotNilf(comp, "%s", table)
			r.Lenf(comp.Order, tcs.groupSize, "%s", table)

			// Verify global ordering is correct.
			r.Equalf(tcs.order, globalOrder(table), "%s", table)

		}

		for _, comp := range sd.Components {
			// Verify order within the component aligns with global order.
			r.True(slices.IsSortedFunc(comp.Order, func(a, b ident.Table) int {
				aIdx := globalOrder(a)
				bIdx := globalOrder(b)
				return aIdx - bIdx
			}))

			r.Equal(len(comp.Order), len(comp.ReverseOrder))
		}
	})

	// Since we have a fully-formed SchemaData, we'll also take this
	// opportunity to test the serialization code. This gets more of a
	// workout in the schemawatch package.
	t.Run("test_serialization", func(t *testing.T) {
		r := require.New(t)

		data, err := sd.MarshalJSON()
		r.NoError(err)
		t.Log(string(data))

		next := &SchemaData{}
		r.NoError(next.UnmarshalJSON(data))

		r.Equal(sd.Columns.Len(), next.Columns.Len())
		_ = sd.Columns.Range(func(table ident.Table, cols []ColData) error {
			found := next.Columns.GetZero(table)
			r.Len(found, len(cols))
			for i := range cols {
				r.True(cols[i].Equal(found[i]))
			}
			return nil
		})
		r.Equal(sd.Dependencies.Len(), next.Dependencies.Len())
		_ = sd.Dependencies.Range(func(table ident.Table, deps []ident.Table) error {
			found := next.Dependencies.GetZero(table)
			r.Len(found, len(deps))
			for i := range deps {
				r.True(ident.Equal(deps[i], found[i]))
			}
			return nil
		})
	})

	t.Run("check_cycle", func(t *testing.T) {
		r := require.New(t)
		cycle1 := ident.NewTable(schema, ident.New("cycle1"))
		cycle2 := ident.NewTable(schema, ident.New("cycle2"))
		refs.Put(cycle1, []ident.Table{cycle2})
		refs.Put(cycle2, []ident.Table{cycle1})
		r.ErrorContains(sd.SetDependencies(refs), "cycle detected")
	})
}
