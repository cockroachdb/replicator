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

// Package ident contains types for safely representing SQL identifiers.
package ident

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// Well-known identifiers.
var (
	Public = New("public") // "public"
)

// An Ident is a quoted SQL identifier, generally a table, column, or
// database. This type is an immutable value type, suitable for use as a
// map key.
type Ident struct {
	q, r string
}

// New returns a quoted SQL identifier. Prefer using ParseIdent when
// operating on user-provided input that may already be quoted.
func New(raw string) Ident {
	return Ident{`"` + strings.ReplaceAll(raw, `"`, `""`) + `"`, raw}
}

// Newf returns a quoted SQL identifier.
func Newf(format string, args ...any) Ident {
	return New(fmt.Sprintf(format, args...))
}

// StagingDB is a type alias for the name of the "_cdc_sink" database.
// It serves as an injection point for uniquely naming the staging
// database in test cases.
type StagingDB Ident

// Ident returns the underying database identifier.
func (s StagingDB) Ident() Ident { return Ident(s) }

// IsEmpty returns true if the identifier is empty.
func (n Ident) IsEmpty() bool {
	return n.r == ""
}

// MarshalJSON returns the Ident's raw form.
func (n Ident) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.Raw())
}

// MarshalText returns the Ident's raw form, allowing the Ident type
// to be used as a JSON map-key.
func (n Ident) MarshalText() ([]byte, error) {
	return []byte(n.Raw()), nil
}

// Raw returns the original, raw value.
func (n Ident) Raw() string {
	return n.r
}

// String returns the ident in a manner suitable for constructing a query.
func (n Ident) String() string { return n.q }

// UnmarshalJSON converts a raw json string into an Ident.
func (n *Ident) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*n = New(raw)
	return nil
}

// UnmarshalText converts a raw string into an Ident.
func (n *Ident) UnmarshalText(data []byte) error {
	*n = New(string(data))
	return nil
}

// A Schema identifier is a two-part ident, consisting of an SQL
// database and schema. This type is an immutable value
// type, suitable for use as a map key.
type Schema struct {
	db, schema Ident
}

// NewSchema constructs a Schema identifier.
func NewSchema(db, schema Ident) Schema {
	return Schema{db, schema}
}

// AsSchema returns the Schema.
func (s Schema) AsSchema() Schema { return s }

// Contains returns true if the given table is defined within the
// user-defined schema.
func (s Schema) Contains(table Table) bool {
	return s.Database() == table.Database() && s.Schema() == table.Schema()
}

// Database returns the schema's enclosing database.
func (s Schema) Database() Ident { return s.db }

// MarshalJSON returns the Schema as a two-element array.
func (s Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{s.Database().Raw(), s.Schema().Raw()})
}

// MarshalText returns the raw, dotted form of the Schema.
func (s Schema) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// Schema returns the schema's name.
func (s Schema) Schema() Ident { return s.schema }

// Raw returns the original, raw value.
func (s Schema) Raw() string {
	return fmt.Sprintf("%s.%s", s.Database().Raw(), s.Schema().Raw())
}

// String returns the identifier in a manner suitable for constructing a
// query.
func (s Schema) String() string {
	return fmt.Sprintf("%s.%s", s.Database(), s.Schema())
}

// UnmarshalJSON parses a two-element array.
func (s *Schema) UnmarshalJSON(data []byte) error {
	parts := make([]Ident, 0, 2)
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	if len(parts) != 2 {
		return errors.Errorf("expecting 2 parts, had %d", len(parts))
	}
	s.db = parts[0]
	s.schema = parts[1]
	return nil
}

// Schematic is anything that can convert itself to a schema.
type Schematic interface {
	// AsSchema returns the value as a Schema.
	AsSchema() Schema
}

var _ Schematic = Schema{}
var _ Schematic = Table{}

// A Table identifier is a three-part ident, consisting of an SQL
// database, schema, and table ident. This type is an immutable value
// type, suitable for use as a map key.
type Table struct {
	db, schema, table Ident
}

// NewTable constructs a Table identifier.
func NewTable(db, schema, table Ident) Table {
	return Table{db, schema, table}
}

// AsSchema returns the schema from the table.
func (t Table) AsSchema() Schema {
	return NewSchema(t.Database(), t.Schema())
}

// Database returns the table's enclosing database.
func (t Table) Database() Ident { return t.db }

// MarshalJSON returns the ident as a three-element array.
func (t Table) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{t.Database().Raw(), t.Schema().Raw(), t.Table().Raw()})
}

// MarshalText returns the raw, dotted form of the Table.
func (t Table) MarshalText() ([]byte, error) {
	return []byte(t.Raw()), nil
}

// Schema returns the table's enclosing schema.
func (t Table) Schema() Ident { return t.schema }

// Table returns the table's identifier.
func (t Table) Table() Ident { return t.table }

// Raw returns the original, raw value.
func (t Table) Raw() string {
	return fmt.Sprintf("%s.%s.%s", t.Database().Raw(), t.Schema().Raw(), t.Table().Raw())
}

// String returns the identifier in a manner suitable for constructing a
// query.
func (t Table) String() string {
	return fmt.Sprintf("%s.%s.%s", t.Database(), t.Schema(), t.Table())
}

// UnmarshalJSON parses a three-element array.
func (t *Table) UnmarshalJSON(data []byte) error {
	parts := make([]Ident, 0, 3)
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	if len(parts) != 3 {
		return errors.Errorf("expecting 3 parts, had %d", len(parts))
	}
	t.db = parts[0]
	t.schema = parts[1]
	t.table = parts[2]
	return nil
}

// A UDT is the name of a user-defined type, such as an enum.
type UDT struct {
	db, schema, udt Ident
}

// NewUDT constructs an identifier for a user-defined type.
func NewUDT(db, schema, name Ident) UDT {
	return UDT{db, schema, name}
}

// AsSchema returns the schema from the UDT.
func (t UDT) AsSchema() Schema {
	return NewSchema(t.Database(), t.Schema())
}

// Database returns the UDT's enclosing database.
func (t UDT) Database() Ident { return t.db }

// MarshalJSON returns the ident as a three-element array.
func (t UDT) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{t.Database().Raw(), t.Schema().Raw(), t.Name().Raw()})
}

// MarshalText returns the raw, dotted form of the UDT.
func (t UDT) MarshalText() ([]byte, error) {
	return []byte(t.Raw()), nil
}

// Schema returns the UDT's enclosing schema.
func (t UDT) Schema() Ident { return t.schema }

// Name returns the UDT's leaf name identifier.
func (t UDT) Name() Ident { return t.udt }

// Raw returns the original, raw value.
func (t UDT) Raw() string {
	return fmt.Sprintf("%s.%s.%s", t.Database().Raw(), t.Schema().Raw(), t.Name().Raw())
}

// String returns the identifier in a manner suitable for constructing a
// query.
func (t UDT) String() string {
	return fmt.Sprintf("%s.%s.%s", t.Database(), t.Schema(), t.Name())
}

// UnmarshalJSON parses a three-element array.
func (t *UDT) UnmarshalJSON(data []byte) error {
	parts := make([]Ident, 0, 3)
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	if len(parts) != 3 {
		return errors.Errorf("expecting 3 parts, had %d", len(parts))
	}
	t.db = parts[0]
	t.schema = parts[1]
	t.udt = parts[2]
	return nil
}
