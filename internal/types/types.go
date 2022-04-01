// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package types contains data types and interfaces that define the
// major functional blocks of code within cdc-sink. The goal of placing
// the types into this package is to make it easy to compose
// functionality as the cdc-sink project evolves.
package types

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgtype/pgxtype"
)

// An Applier accepts some number of Mutations and applies them to
// a target table.
type Applier interface {
	Apply(context.Context, pgxtype.Querier, []Mutation) error
}

// Appliers is a factory for Applier instances.
type Appliers interface {
	Get(ctx context.Context, target ident.Table, casColumns []ident.Ident, deadlines Deadlines) (Applier, error)
}

// An Authenticator determines if an operation on some schema should be
// allowed to proceed.
type Authenticator interface {
	// Check returns true if a request containing some bearer token
	// should be allowed to operate on the given schema.
	Check(ctx context.Context, schema ident.Schema, token string) (ok bool, _ error)
}

// Deadlines associate a column identifier with a duration.
type Deadlines map[ident.Ident]time.Duration

// A Mutation describes a row to upsert into the target database.  That
// is, it is a collection of column values to apply to a row in some
// table.
type Mutation struct {
	Data json.RawMessage // An encoded JSON object: { "key" : "hello" }
	Key  json.RawMessage // An encoded JSON array: [ "hello" ]
	Time hlc.Time        // The effective time of the mutation
}

var nullBytes = []byte("null")

// IsDelete returns true if the Mutation represents a deletion.
func (m Mutation) IsDelete() bool {
	return len(m.Data) == 0 || bytes.Equal(m.Data, nullBytes)
}

// Stager describes a service which can durably persist some
// number of Mutations.
type Stager interface {
	// Drain will delete queued mutations. It is not idempotent.
	Drain(ctx context.Context, tx pgxtype.Querier, prev, next hlc.Time) ([]Mutation, error)

	// Store implementations should be idempotent.
	Store(ctx context.Context, db pgxtype.Querier, muts []Mutation) error
}

// Stagers is a factory for Stager instances.
type Stagers interface {
	Get(ctx context.Context, target ident.Table) (Stager, error)
}

// A TimeKeeper maintains a durable map of string keys to timestamps.
type TimeKeeper interface {
	// Put stores a new timestamp for the given key, returning the
	// previous value. If no previous value was present, hlc.Zero() will
	// be returned.
	Put(context.Context, pgxtype.Querier, ident.Schema, hlc.Time) (hlc.Time, error)
}

// ColData hold SQL column metadata.
type ColData struct {
	Ignored bool
	Name    ident.Ident
	Primary bool
	Type    string
}

// TableSchema maintains the columns in a table.
type TableSchema struct {
	Columns []ColData
}

// DatabaseTables maintains the tables in a database.
type DatabaseTables struct {
	Tables map[ident.Table]TableSchema
	// The TablesSortedByFK field holds the tables in an order that satisfies FK constraints.
	TablesSortedByFK []ident.Table
}

// Watcher allows table metadata to be observed.
//
// The methods in this type return column data such that primary key
// columns are returned first, in their declaration order, followed
// by all other non-pk columns.
type Watcher interface {
	// Refresh will force the Watcher to immediately query the database
	// for updated schema information. This is intended for testing and
	// does not need to be called in the general case.
	Refresh(context.Context, pgxtype.Querier) error
	// Snapshot returns the tables known to be part of a database.
	Snapshot() DatabaseTables
	// Snapshot returns the tables known to be part of a database.
	Watch(table ident.Table) (_ <-chan []ColData, cancel func(), _ error)
}

// Watchers is a factory for Watcher instances.
type Watchers interface {
	Get(ctx context.Context, db ident.Ident) (Watcher, error)
}
