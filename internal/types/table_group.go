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
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// A TableGroup is a named collection of tables. This is properly called
// a "schema", but that noun is overloaded.
type TableGroup struct {
	// A (globally-unique) name for the group that is used for
	// mutual-exclusion. It is often, but not required to be, the
	// enclosing database schema for the table.
	Name ident.Ident
	// Optional. If present, this field indicates that the group
	// comprises the entire schema, implying known and unknown tables.
	Enclosing ident.Schema
	// The tables which comprise the group.
	Tables []ident.Table
}

// Schema returns a common schema for the group, either
// [TableGroup.Enclosing] or a schema common to all configured tables.
func (g *TableGroup) Schema() (ident.Schema, error) {
	if !g.Enclosing.Empty() {
		return g.Enclosing, nil
	}
	var commonSchema ident.Schema
	for idx, tbl := range g.Tables {
		if idx == 0 {
			commonSchema = tbl.Schema()
		} else if !ident.Equal(tbl.Schema(), commonSchema) {
			// This represents a limitation that we could lift
			// with TableGroups being explicitly configured.
			return ident.Schema{}, errors.Errorf("mixed-schemas in group: %s vs %s",
				commonSchema, tbl.Schema())
		}
	}
	return commonSchema, nil
}

// String is for debugging use only.
func (g *TableGroup) String() string {
	var sb strings.Builder
	sb.WriteString(g.Name.Canonical().String())
	sb.WriteString(" [ ")
	for idx, table := range g.Tables {
		if idx > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(table.Canonical().String())
	}
	sb.WriteString(" ]")
	return sb.String()
}
