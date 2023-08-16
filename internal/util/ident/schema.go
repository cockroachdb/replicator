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

// A Schema identifier is a multipart identifier for a Table container.
// This type is immutable and suitable for use as a map key.
type Schema struct {
	_ noCompare
	*array
}

// MustSchema calls NewSchema and panics if it returns an error. This is
// intended for use by tests.
func MustSchema(parts ...Ident) Schema {
	ret, err := NewSchema(parts...)
	if err != nil {
		panic(err)
	}
	return ret
}

// NewSchema constructs a Schema identifier.
func NewSchema(parts ...Ident) (Schema, error) {
	if len(parts) == 0 {
		return Schema{}, nil
	}
	if len(parts) > maxArrayLength {
		return Schema{}, errors.Errorf("number of parts %d exceeds maximum %d", len(parts), maxArrayLength)
	}
	var key arrayKey
	for idx, part := range parts {
		key[idx] = part.atom
	}
	return Schema{array: arrays.Get(key)}, nil
}

// Canonical returns a canonicalized form of the SQL schema name. That
// is, it returns a lower-cased form of the enclosed SQL identifiers.
func (s Schema) Canonical() Schema {
	if s.array == nil {
		return Schema{}
	}
	return Schema{array: s.array.lowered}
}

// Contains returns true if the given table is defined within the
// Schema.
func (s Schema) Contains(table Table) bool {
	sParts := s.Idents(make([]Ident, 0, maxArrayLength))
	tParts := table.Idents(make([]Ident, 0, maxArrayLength+1))

	// Empty schema contains no tables.
	if len(sParts) == 0 {
		return false
	}
	// The table must have an enclosing namespace (i.e. have more parts
	// than a schema which would enclose it).
	if len(sParts) >= len(tParts) {
		return false
	}
	// The schema parts should then be a prefix of the table parts.
	for idx, sPart := range sParts {
		if !Equal(sPart, tParts[idx]) {
			return false
		}
	}
	return true
}

// Relative returns a new schema, relative to the receiver. For an input
// of N parts, the trailing N elements of the receiver will be replaced.
func (s Schema) Relative(parts ...Ident) (Schema, Qualification, error) {
	existingSchemaParts := make([]Ident, 0, maxArrayLength)
	existingSchemaParts = s.Idents(existingSchemaParts)

	if len(parts) > len(existingSchemaParts) {
		return Schema{}, 0, errors.Errorf(
			"expecting no more than %d schema parts, saw %d",
			len(existingSchemaParts), len(parts))
	}

	if len(parts) == len(existingSchemaParts) {
		ret, err := NewSchema(parts...)
		return ret, FullyQualified, err
	}

	copy(existingSchemaParts[len(existingSchemaParts)-len(parts):], parts)

	ret, err := NewSchema(existingSchemaParts...)
	return ret, PartialSchema, err
}

// Schema implements Schematic.
func (s Schema) Schema() Schema { return s }

// UnmarshalJSON parses an array of strings.
func (s *Schema) UnmarshalJSON(data []byte) error {
	parts := make([]string, 0, maxArrayLength)
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	if len(parts) == 0 {
		// Empty.
		*s = Schema{}
		return nil
	}
	if len(parts) > maxArrayLength {
		return errors.Errorf("expecting at most %d parts, had %d", maxArrayLength, len(parts))
	}
	var key arrayKey
	for idx, part := range parts {
		key[idx] = atoms.Get(part)
	}
	s.array = arrays.Get(key)
	return nil
}

// Schematic is anything that is or is enclosed by a Schema.
type Schematic interface {
	// Schema returns the (enclosing) Schema.
	Schema() Schema
}

var (
	_ Schematic = Schema{}
	_ Schematic = Table{}
	_ Schematic = UDT{}
)
