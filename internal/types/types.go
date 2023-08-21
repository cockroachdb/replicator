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

// Package types contains data types and interfaces that define the
// major functional blocks of code within cdc-sink. The goal of placing
// the types into this package is to make it easy to compose
// functionality as the cdc-sink project evolves.
package types

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// An Applier accepts some number of Mutations and applies them to
// a target table.
type Applier interface {
	Apply(context.Context, TargetQuerier, []Mutation) error
}

// Appliers is a factory for Applier instances.
type Appliers interface {
	Get(ctx context.Context, target ident.Table) (Applier, error)
}

// An Authenticator determines if an operation on some schema should be
// allowed to proceed.
type Authenticator interface {
	// Check returns true if a request containing some bearer token
	// should be allowed to operate on the given schema.
	Check(ctx context.Context, schema ident.Schema, token string) (ok bool, _ error)
}

// Deadlines associate a column identifier with a duration.
type Deadlines = *ident.Map[time.Duration]

var (
	// ErrCancelSingleton may be returned by callbacks passed to
	// leases.Singleton to shut down cleanly.
	ErrCancelSingleton = errors.New("singleton requested cancellation")
)

// A Lease represents a time-based, exclusive lock.
type Lease interface {
	// Context will be canceled when the lease has expired.
	Context() context.Context

	// Release terminates the Lease.
	Release()
}

// LeaseBusyError is returned by [Leases.Acquire] if another caller
// holds the lease.
type LeaseBusyError struct {
	Expiration time.Time
}

func (e *LeaseBusyError) Error() string { return "lease is held by another caller" }

// IsLeaseBusy returns the error if it represents a busy lease.
func IsLeaseBusy(err error) (busy *LeaseBusyError, ok bool) {
	return busy, errors.As(err, &busy)
}

// Leases coordinates behavior across multiple instances of cdc-sink.
type Leases interface {
	// Acquire the named lease. A [LeaseBusyError] will be returned if
	// another caller has already acquired the lease.
	Acquire(ctx context.Context, name string) (Lease, error)

	// Singleton executes a callback when the named lease is acquired.
	//
	// The lease will be released in the following circumstances:
	//   * The callback function returns.
	//   * The lease cannot be renewed before it expires.
	//   * The outer context is canceled.
	//
	// If the callback returns a non-nil error, the error will be
	// logged. If the callback returns ErrCancelSingleton, it will not
	// be retried. In all other cases, the callback function is retried
	// once a lease is re-acquired.
	Singleton(ctx context.Context, name string, fn func(ctx context.Context) error)
}

// A Memo is a key store that persists a value associated to a key
type Memo interface {
	// Get retrieves the value associate to the given key.
	// If the value is not found, a nil slice is returned.
	Get(ctx context.Context, tx StagingQuerier, key string) ([]byte, error)
	// Put stores a value associated to the key.
	Put(ctx context.Context, tx StagingQuerier, key string, value []byte) error
}

// A Mutation describes a row to upsert into the target database.  That
// is, it is a collection of column values to apply to a row in some
// table.
type Mutation struct {
	Data json.RawMessage // An encoded JSON object: { "key" : "hello" }
	Key  json.RawMessage // An encoded JSON array: [ "hello" ]
	Time hlc.Time        // The effective time of the mutation
	Meta map[string]any  // Dialect-specific data, may be nil
}

var nullBytes = []byte("null")

// IsDelete returns true if the Mutation represents a deletion.
func (m Mutation) IsDelete() bool {
	return len(m.Data) == 0 || bytes.Equal(m.Data, nullBytes)
}

// Stager describes a service which can durably persist some
// number of Mutations.
type Stager interface {
	// Retire will delete staged mutations whose timestamp is less than
	// or equal to the given end time. Note that this call may take an
	// arbitrarily long amount of time to complete and its effects may
	// not occur within a single database transaction.
	Retire(ctx context.Context, db StagingQuerier, end hlc.Time) error

	// Select will return all queued mutations between the timestamps.
	Select(ctx context.Context, tx StagingQuerier, prev, next hlc.Time) ([]Mutation, error)

	// SelectPartial will return queued mutations between the
	// timestamps. The after and limit arguments are used together when
	// backfilling large amounts of data.
	SelectPartial(ctx context.Context, tx StagingQuerier, prev, next hlc.Time, afterKey []byte, limit int) ([]Mutation, error)

	// Store implementations should be idempotent.
	Store(ctx context.Context, db StagingQuerier, muts []Mutation) error

	// TransactionTimes returns  distinct timestamps in the range
	// (after, before] for which there is data in the associated table.
	TransactionTimes(ctx context.Context, tx StagingQuerier, before, after hlc.Time) ([]hlc.Time, error)
}

// SelectManyCallback is provided to Stagers.SelectMany to receive the
// incoming data.
type SelectManyCallback func(ctx context.Context, tbl ident.Table, mut Mutation) error

// SelectManyCursor is used with Stagers.SelectMany. The After values
// will be updated by the method, allowing callers to call SelectMany
// in a loop until fewer than Limit values are returned.
type SelectManyCursor struct {
	Start, End hlc.Time
	Targets    [][]ident.Table // The outer slice defines FK groupings.
	Limit      int

	// If true, we read all updates for parent tables before children,
	// but make no guarantees around transactional boundaries. If false,
	// we read some number of individual MVCC timestamps in their
	// entirety.
	Backfill bool

	OffsetKey   json.RawMessage
	OffsetTable ident.Table
	OffsetTime  hlc.Time
}

// Stagers is a factory for Stager instances.
type Stagers interface {
	Get(ctx context.Context, target ident.Table) (Stager, error)

	// SelectMany performs queries across multiple staging tables, to
	// more readily support backfilling large amounts of data that may
	// result from a changefeed's initial_scan.
	//
	// This method will update the fields within the cursor so that it
	// can be used to restart in case of interruption.
	SelectMany(ctx context.Context, tx StagingQuerier, q *SelectManyCursor, fn SelectManyCallback) error
}

// ColData hold SQL column metadata.
type ColData struct {
	Ignored bool
	Name    ident.Ident
	// A Parse function may be supplied to allow a string representation
	// of a complex datatype to be converted into a type more readily
	// used by a target database driver.
	Parse   func(string) (any, bool) `json:"-"`
	Primary bool
	// Type of the column.
	Type string
}

// Equal returns true if the two ColData are equivalent under
// case-insensitivity.
func (d ColData) Equal(o ColData) bool {
	return d.Ignored == o.Ignored &&
		ident.Equal(d.Name, o.Name) &&
		// Parse is excluded, since functions are not comparable.
		d.Primary == o.Primary &&
		d.Type == o.Type
}

// SchemaData holds SQL schema metadata.
type SchemaData struct {
	Columns *ident.TableMap[[]ColData]

	// Order is a two-level slice that represents equivalency-groups
	// with respect to table foreign-key ordering. That is, if all
	// updates for tables in Order[N] are applied, then updates in
	// Order[N+1] can then be applied.
	//
	// The need for this data can be revisited if CRDB adds support
	// for deferrable foreign-key constraints:
	// https://github.com/cockroachdb/cockroach/issues/31632
	Order [][]ident.Table
}

// OriginalName returns the name of the table as it is defined in the
// underlying database.
func (s *SchemaData) OriginalName(tbl ident.Table) (ident.Table, bool) {
	ret, _, ok := s.Columns.Match(tbl)
	return ret, ok
}

// Product is an enum type to make it easy to switch on the underlying
// database.
type Product int

//go:generate go run golang.org/x/tools/cmd/stringer -type=Product -trimprefix Product

// These are various product types that we support.
const (
	ProductUnknown Product = iota
	ProductCockroachDB
	ProductOracle
	ProductPostgreSQL
)

// ExpandSchema validates a Schema against the expected form used by the
// database. This method may return an alternate value to add missing
// default namespace elements.
func (p Product) ExpandSchema(s ident.Schema) (ident.Schema, error) {
	if s.Empty() {
		return ident.Schema{}, errors.New("empty schema not allowed")
	}

	parts := s.Idents(nil)
	numParts := len(parts)

	switch p {
	case ProductCockroachDB, ProductPostgreSQL:
		switch numParts {
		case 1:
			// Add missing "public" identifier.
			return ident.NewSchema(parts[0], ident.Public)
		case 2:
			// Fully-specified, return as-is.
			return s, nil
		default:
			return ident.Schema{}, errors.Errorf("unexpected number of schema parts: %d", numParts)
		}

	case ProductOracle:
		if numParts != 1 {
			return ident.Schema{}, errors.Errorf("expecting exactly one schema part, had %d", numParts)
		}
		return s, nil

	default:
		return ident.Schema{}, errors.Errorf("unimplemented: %s", p)
	}
}

// AnyPool is a generic type constraint for any database pool type
// that we support.
type AnyPool interface {
	*SourcePool | *StagingPool | *TargetPool
	Info() *PoolInfo
}

// StagingQuerier is implemented by pgxpool.Pool, pgxpool.Conn, pgxpool.Tx,
// pgx.Conn, and pgx.Tx types. This allows a degree of flexibility in
// defining types that require a database connection.
type StagingQuerier interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) pgx.Row
}

var (
	_ StagingQuerier = (*pgxpool.Conn)(nil)
	_ StagingQuerier = (*pgxpool.Pool)(nil)
	_ StagingQuerier = (*pgxpool.Tx)(nil)
	_ StagingQuerier = (*pgx.Conn)(nil)
	_ StagingQuerier = (pgx.Tx)(nil)
)

// PoolInfo describes a database connection pool and what it's connected
// to.
type PoolInfo struct {
	ConnectionString string
	Product          Product
	Version          string
}

// Info returns the PoolInfo when embedded.
func (i *PoolInfo) Info() *PoolInfo { return i }

// StagingPool is an injection point for a connection to the staging database.
type StagingPool struct {
	*pgxpool.Pool
	PoolInfo
	_ noCopy
}

// SourcePool is an injection point for a connection to a source
// database.
type SourcePool struct {
	*sql.DB
	PoolInfo
	_ noCopy
}

// TargetPool is an injection point for a connection to the target database.
type TargetPool struct {
	*sql.DB
	PoolInfo
	_ noCopy
}

// TargetQuerier is implemented by [sql.DB] and [sql.Tx].
type TargetQuerier interface {
	ExecContext(ctx context.Context, sql string, arguments ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, sql string, optionsAndArgs ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, sql string, optionsAndArgs ...interface{}) *sql.Row
}

var (
	_ TargetQuerier = (*sql.DB)(nil)
	_ TargetQuerier = (*sql.Tx)(nil)
)

// TargetTx is implemented by [sql.Tx].
type TargetTx interface {
	TargetQuerier
	Commit() error
	Rollback() error
}

var _ TargetTx = (*sql.Tx)(nil)

// Watcher allows table metadata to be observed.
//
// The methods in this type return column data such that primary key
// columns are returned first, in their declaration order, followed
// by all other non-pk columns.
type Watcher interface {
	// Get returns a snapshot of all tables in the target database.
	// The returned struct must not be modified.
	Get() *SchemaData
	// Refresh will force the Watcher to immediately query the database
	// for updated schema information. This is intended for testing and
	// does not need to be called in the general case.
	Refresh(context.Context, *TargetPool) error
	// Watch returns a channel that emits updated column data for the
	// given table.  The channel will be closed if there
	Watch(table ident.Table) (_ <-chan []ColData, cancel func(), _ error)
}

// Watchers is a factory for Watcher instances.
type Watchers interface {
	Get(ctx context.Context, db ident.Schema) (Watcher, error)
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
