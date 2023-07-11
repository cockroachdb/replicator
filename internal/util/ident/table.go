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

	"github.com/pkg/errors"
)

// A Table is an identifier with an enclosing Schema. This type is an
// immutable value type, suitable for use as a map key.
type Table struct {
	*qualified
}

// NewTable constructs a Table identifier.
func NewTable(enclosing Schema, name Ident) Table {
	if enclosing.Empty() && name.Empty() {
		return Table{}
	}
	return Table{qualifieds.Get(qualifiedKey{enclosing.array, name.atom})}
}

// Schema implements Schematic by returning the enclosing Schema.
func (t Table) Schema() Schema {
	if t.qualified == nil {
		return Schema{}
	}
	return Schema{t.qualified.namespace}
}

// Table returns the table's identifier.
func (t Table) Table() Ident {
	if t.qualified == nil {
		return Ident{}
	}
	return Ident{t.qualified.terminal}
}

// UnmarshalJSON parses a JSON array.
func (t *Table) UnmarshalJSON(data []byte) error {
	parts := make([]Ident, 0, maxArrayLength+1)
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	switch len(parts) {
	case 0:
		// []
		*t = Table{}
	case 1:
		// [ "table" ]
		*t = NewTable(Schema{}, parts[0])
	case 2:
		// [ "my_schema", "table" ]
		sch, err := NewSchema(parts[0])
		if err != nil {
			return err
		}
		*t = NewTable(sch, parts[1])
	case 3:
		// [ "my_db", "my_schema", "table" ]
		sch, err := NewSchema(parts[0], parts[1])
		if err != nil {
			return err
		}
		*t = NewTable(sch, parts[2])
	default:
		return errors.Errorf("expecting at most %d parts, had %d", maxArrayLength+1, len(parts))
	}
	return nil
}

// UnmarshalText initializes the Table from its MarshalText
// representation.
func (t *Table) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		*t = Table{}
		return nil
	}
	ret, err := ParseTable(string(data))
	if err == nil {
		*t = ret
	}
	return err
}
