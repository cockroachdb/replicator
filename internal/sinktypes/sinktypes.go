// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sinktypes contains data types and interfaces that define the
// major functional blocks of code within cdc-sink. The goal of placing
// the types into this package is to make it easy to compose
// functionality as the cdc-sink project evolves.
package sinktypes

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
)

// An Applier accepts some number of Mutations and applies them to
// a target table.
type Applier interface {
	Apply(context.Context, Batcher, []Mutation) error
}

// Appliers is a factory for Applier instances.
type Appliers interface {
	Get(ctx context.Context, target ident.Table) (Applier, error)
}

// A Batcher allows for a batch of statements to be executed in a single
// round-trip to the database. This is implemented by several pgx types,
// such as pgxpool.Pool and pgx.Tx.
type Batcher interface {
	pgxtype.Querier
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
}

// A Mutation describes a row to upsert into the target database.  That
// is, it is a collection of column values to apply to a row in some
// table.
type Mutation struct {
	Data json.RawMessage // An encoded JSON object: { "key" : "hello" }
	Key  json.RawMessage // An encoded JSON array: [ "hello" ]
	Time hlc.Time        // The effective time of the mutation
}

var nullBytes = []byte("null")

// Delete returns true if the Mutation represents a deletion.
func (m Mutation) Delete() bool {
	return len(m.Data) == 0 || bytes.Equal(m.Data, nullBytes)
}

// MutationStore describes a service which can durably persist some
// number of Mutations.
type MutationStore interface {
	// Drain will delete queued mutations. It is not idempotent.
	Drain(ctx context.Context, tx pgxtype.Querier, prev, next hlc.Time) ([]Mutation, error)

	// Store implementations should be idempotent.
	Store(ctx context.Context, db Batcher, muts []Mutation) error
}

// MutationStores is a factory for MutationStore instances.
type MutationStores interface {
	Get(ctx context.Context, target ident.Table) (MutationStore, error)
}

// A TimeSwapper maintains a durable map of string keys to timestamps.
type TimeSwapper interface {
	// Swap stores a new timestamp for the given key, returning the
	// previous value. If no previous value was present, hlc.Zero() will
	// be returned.
	Swap(context.Context, pgxtype.Querier, string, hlc.Time) (hlc.Time, error)
}

// ColData hold SQL column metadata.
type ColData struct {
	Name    ident.Ident
	Primary bool
	Type    string
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
	// Snapshot returns the latest known schema for all tables.
	Snapshot() map[ident.Table][]ColData
	// Watch returns a channel that emits updated column data for
	// the given table.  The channel will be closed if there
	Watch(table ident.Table) (_ <-chan []ColData, cancel func(), _ error)
}

// Watchers is a factory for Watcher instances.
type Watchers interface {
	Get(ctx context.Context, db ident.Ident) (Watcher, error)
}
