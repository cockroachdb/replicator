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
	"strings"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// SchemaComponent represents a strongly-connected component based
// on foreign-key relationships.
type SchemaComponent struct {
	// Order is sorted such that parent tables will appear before child
	// tables.
	Order []ident.Table

	// ReverseOrder is sorted such that child tables will appear before
	// parent tables.
	ReverseOrder []ident.Table
}

// sort the tables by level and then by name.
func (c *SchemaComponent) sort(levels *ident.TableMap[int]) error {
	slices.SortFunc(c.Order, func(a, b ident.Table) int {
		if c := levels.GetZero(a) - levels.GetZero(b); c != 0 {
			return c
		}
		return strings.Compare(a.Canonical().Raw(), b.Canonical().Raw())
	})

	c.ReverseOrder = slices.Clone(c.Order)
	slices.Reverse(c.ReverseOrder)
	return nil
}

// SchemaData holds SQL schema metadata.
type SchemaData struct {
	// Columns describes table columns. Primary keys will appear first
	// in the slice, in table-column order. The remaining columns will
	// be sorted by name.
	Columns *ident.TableMap[[]ColData]

	// Components describes strongly-connected component relationships
	// of the tables in the schema. That is, each element represents a
	// group of tables that have a FK relationship that must be handled
	// in a transactionally-consistent fashion.
	Components []*SchemaComponent

	// Entire is a flattened view of all Components.
	Entire *SchemaComponent

	// TableComponents is an index into Components.
	TableComponents *ident.TableMap[*SchemaComponent]
}

// SetComponents initializes the Components and Entire fields.
func (s *SchemaData) SetComponents(parentsToChildren *ident.TableMap[[]ident.Table]) error {
	allTables := &ident.TableMap[struct{}]{}
	isChild := &ident.TableMap[bool]{}
	_ = parentsToChildren.Range(func(table ident.Table, children []ident.Table) error {
		allTables.Put(table, struct{}{})
		for _, child := range children {
			allTables.Put(child, struct{}{})

			// Don't treat self-referential tables as a child.
			if !ident.Equal(table, child) {
				isChild.Put(child, true)
			}
		}
		return nil
	})

	// Recursively assign tables into groups and levels, starting with
	// root tables.
	assigned := &ident.TableMap[bool]{}
	assignments := &ident.TableMap[*SchemaComponent]{}
	levels := &ident.TableMap[int]{}
	_ = parentsToChildren.Range(func(table ident.Table, _ []ident.Table) error {
		if !isChild.GetZero(table) {
			assign(table, table, parentsToChildren, assigned, assignments, levels, 0)
		}
		return nil
	})

	s.Components = make([]*SchemaComponent, 0, assignments.Len())
	s.Entire = &SchemaComponent{}
	s.TableComponents = &ident.TableMap[*SchemaComponent]{}

	// Unpack the assigned groups, sorting the tables by dependency
	// order.
	if err := assignments.Range(func(_ ident.Table, comp *SchemaComponent) error {
		s.Components = append(s.Components, comp)
		s.Entire.Order = append(s.Entire.Order, comp.Order...)
		for _, table := range comp.Order {
			s.TableComponents.Put(table, comp)
		}
		return comp.sort(levels)
	}); err != nil {
		return err
	}

	// Ensure that all input tables have been assigned to a component.
	// Tables involved in a reference cycle will
	if s.TableComponents.Len() != allTables.Len() {
		var sb strings.Builder
		_ = allTables.Range(func(table ident.Table, _ struct{}) error {
			if _, found := s.TableComponents.Get(table); !found {
				if sb.Len() > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(table.String())
			}
			return nil
		})
		return errors.Errorf("cycle detected in tables: %s", sb.String())
	}
	if err := s.Entire.sort(levels); err != nil {
		return err
	}

	// Sort components by shortest-first and then by name of 0th element
	// to ensure stable output. This works because we know that a table
	// cannot be in two groups at once.
	slices.SortFunc(s.Components, func(a, b *SchemaComponent) int {
		if c := len(a.Order) - len(b.Order); c != 0 {
			return c
		}
		return strings.Compare(
			a.Order[0].Canonical().Raw(),
			b.Order[0].Canonical().Raw())
	})
	return nil
}

// OriginalName returns the name of the table as it is defined in the
// underlying database.
func (s *SchemaData) OriginalName(tbl ident.Table) (ident.Table, bool) {
	ret, _, ok := s.Columns.Match(tbl)
	return ret, ok
}

// assign recursively aggregates the tables into groups reachable from
// some particular root. It also tracks the maximum referential depth
// for any given table. We don't need to worry about cyclical table
// references, since a cyclic table wouldn't have been classified as a
// root. This is, essentially, the second half of Kosaraju's algorithm
// with traversal-depth. We already have a complete pre-order of the
// root tables, so we can skip the visit phase.
func assign(
	table, root ident.Table,
	parentsToChildren *ident.TableMap[[]ident.Table],
	assigned *ident.TableMap[bool],
	assignments *ident.TableMap[*SchemaComponent],
	levels *ident.TableMap[int],
	level int,
) {

	// We always want to increase a table's level.
	levels.Put(table, max(level, levels.GetZero(table)))

	// Process tables once.
	if assigned.GetZero(table) {
		return
	}
	assigned.Put(table, true)

	// Add table to its component.
	assignment, ok := assignments.Get(root)
	if !ok {
		assignment = &SchemaComponent{}
		assignments.Put(root, assignment)
	}
	assignment.Order = append(assignment.Order, table)

	// Recurse over child tables.
	for _, child := range parentsToChildren.GetZero(table) {
		assign(child, root, parentsToChildren, assigned, assignments, levels, level+1)
	}
}
