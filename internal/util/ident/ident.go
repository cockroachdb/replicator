// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ident contains types for safely representing SQL identifiers.
package ident

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// Well-known identifiers.
var (
	StagingDb = New("_cdc_sink") // "_cdc_sink"
	Public    = New("public")    // "public"
	Resolved  = NewTable(StagingDb, Public, New("resolved"))
)

// An Ident is a quoted SQL identifier, generally a table, column, or
// database. This type is an immutable value type, suitable for use as a
// map key.
type Ident struct {
	q string
}

// New returns a quoted SQL identifier. This method will panic if an
// empty string is passed.
func New(raw string) Ident {
	return Ident{`"` + strings.ReplaceAll(raw, `"`, `""`) + `"`}
}

// Newf returns a quoted SQL identifier.
func Newf(format string, args ...interface{}) Ident {
	return New(fmt.Sprintf(format, args...))
}

// Relative parses a table name and returns a fully-qualified Table
// name whose database value is always db.
//
// If the input table name is a simple string or has exactly two parts,
// the resulting Table will have the form "db.public.table".
//
// If the input table has three parts, it will be interpreted as a
// fully-qualified
func Relative(db Ident, table string) (Table, error) {
	if table == "" {
		return Table{}, errors.New("empty table")
	}

	parts := strings.Split(table, ".")
	switch len(parts) {
	case 1:
		return Table{db, Public, New(parts[0])}, nil
	case 2:
		return Table{db, Public, New(parts[1])}, nil
	case 3:
		return Table{db, New(parts[1]), New(parts[2])}, nil
	default:
		return Table{}, errors.Errorf("too many parts in %q", table)
	}
}

// Empty returns true if the identifier is empty.
func (n Ident) Empty() bool {
	return n.q == `""`
}

// Raw returns the original, raw value.
func (n Ident) Raw() string {
	return strings.ReplaceAll(n.q[1:len(n.q)-1], `""`, `"`)
}

// String returns the ident in a manner suitable for constructing a query.
func (n Ident) String() string { return n.q }

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

// Database returns the table's enclosing database.
func (t Table) Database() Ident { return t.db }

// Schema returns the table's enclosing schema.
func (t Table) Schema() Ident { return t.schema }

// Table returns the table's identifier.
func (t Table) Table() Ident { return t.table }

// String returns the identifier in a manner suitable for constructing a
// query.
func (t Table) String() string {
	return fmt.Sprintf("%s.%s.%s", t.Database(), t.Schema(), t.Table())
}
