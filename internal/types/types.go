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
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stmtcache"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// An Authenticator determines if an operation on some schema should be
// allowed to proceed.
type Authenticator interface {
	// Check returns true if a request containing some bearer token
	// should be allowed to operate on the given schema.
	Check(ctx context.Context, schema ident.Schema, token string) (ok bool, _ error)
}

// Deadlines associate a column identifier with a duration.
type Deadlines = *ident.Map[time.Duration]

// A DLQ is a dead-letter queue that allows mutations to be written
// to the target for offline reconciliation.
type DLQ interface {
	Enqueue(ctx context.Context, tx TargetQuerier, mut Mutation) error
}

// DLQs provides named dead-letter queues in the target schema.
type DLQs interface {
	Get(ctx context.Context, target ident.Schema, name string) (DLQ, error)
}

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

// CustomUpsert defines a custom upsert template that will
// be used for the mutation.
// A mutation may specify a custom upsert by adding
// an entry in the Meta map below
// Meta[CustomUpsert] = "custom.template.name"
const CustomUpsert = "upsert.custom"

// A Mutation describes a row to upsert into the target database.  That
// is, it is a collection of column values to apply to a row in some
// table.
type Mutation struct {
	Before json.RawMessage // Optional encoded JSON object
	Data   json.RawMessage // An encoded JSON object: { "key" : "hello" }
	Key    json.RawMessage // An encoded JSON array: [ "hello" ]
	Meta   map[string]any  // Dialect-specific data, may be nil, not persisted
	Time   hlc.Time        // The effective time of the mutation
}

var nullBytes = []byte("null")

// IsDelete returns true if the Mutation represents a deletion.
func (m Mutation) IsDelete() bool {
	return len(m.Data) == 0 || bytes.Equal(m.Data, nullBytes)
}

// Stager describes a service which can durably persist some
// number of Mutations.
type Stager interface {
	// MarkApplied will mark the given mutations as having been applied.
	// This is used with lease-based unstaging or when certain mutation
	// should be skipped.
	MarkApplied(ctx context.Context, db StagingQuerier, muts []Mutation) error

	// Retire will delete staged mutations whose timestamp is less than
	// or equal to the given end time. Note that this call may take an
	// arbitrarily long amount of time to complete and its effects may
	// not occur within a single database transaction.
	Retire(ctx context.Context, db StagingQuerier, end hlc.Time) error

	// Stage writes the mutations into the staging table. This method is
	// idempotent.
	Stage(ctx context.Context, db StagingQuerier, muts []Mutation) error

	// StageIfExists will stage a mutation only if there is already a
	// mutation staged for its key. It returns a filtered copy of the
	// mutations that were not staged. This method is used to implement
	// the non-transactional mode, where we try to apply a mutation to
	// some key if there isn't already a mutation queued for that key.
	StageIfExists(ctx context.Context, db StagingQuerier, muts []Mutation) ([]Mutation, error)
}

// UnstageCallback is provided to [Stagers.Unstage] to receive the
// incoming data.
type UnstageCallback func(ctx context.Context, tbl ident.Table, mut Mutation) error

// UnstageCursor is used with Stagers.SelectMany. The After values
// will be updated by the method, allowing callers to call Unstage
// in a loop until it returns false.
type UnstageCursor struct {
	// If true, un-applied mutations with an active lease will be
	// returned. This is intended for final cleanups and testing.
	IgnoreLeases bool

	// If non-zero, the retrieved mutations will be marked with a
	// lease-expiration time, rather than being marked as applied.
	// Callers making use of lease expirations must make a subsequent
	// call to [Stagers.MarkApplied] to prevent the mutation from
	// being applied later.
	LeaseExpiry time.Time

	// A half-open interval: [ StartAt, EndBefore )
	StartAt, EndBefore hlc.Time

	// TableOffsets is used when processing very large batches that
	// occur within a single timestamp, to provide an additional offset
	// for skipping already-processed rows. The implementation of
	// [Stagers.Unstage] will automatically populate this field.
	TableOffsets ident.TableMap[UnstageOffset]

	// Targets defines the order in which data for the selected tables
	// will be passed to the results callback.
	Targets []ident.Table

	// Limit the number of distinct MVCC timestamps that are returned.
	// This will default to a single timestamp if unset.
	TimestampLimit int

	// Limit the number of rows that are selected from any given staging
	// table. The total number of rows that can be emitted is the limit
	// multiplied by the number of tables listed in Targets. This will
	// return all rows within the selected timestamp(s) if unset.
	UpdateLimit int
}

// String is for debugging use only.
func (c *UnstageCursor) String() string {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", " ")
	if err := enc.Encode(c); err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return buf.String()
}

// UnstageOffset is used within an [UnstageCursor] to provide
// fine-grained pagination within the records for a single table.
type UnstageOffset struct {
	Key  json.RawMessage
	Time hlc.Time
}

func (o *UnstageOffset) String() string {
	return fmt.Sprintf("%s @ %s", o.Key, o.Time)
}

// Copy returns a copy of the cursor so that it may be updated.
func (c *UnstageCursor) Copy() *UnstageCursor {
	cpy := &UnstageCursor{
		EndBefore:      c.EndBefore,
		StartAt:        c.StartAt,
		IgnoreLeases:   c.IgnoreLeases,
		LeaseExpiry:    c.LeaseExpiry,
		Targets:        make([]ident.Table, len(c.Targets)),
		TimestampLimit: c.TimestampLimit,
		UpdateLimit:    c.UpdateLimit,
	}
	c.TableOffsets.CopyInto(&cpy.TableOffsets)
	copy(cpy.Targets, c.Targets)
	return cpy
}

// Stagers is a factory for Stager instances.
type Stagers interface {
	Get(ctx context.Context, target ident.Table) (Stager, error)

	// Unstage implements an exactly-once behavior of reading staged
	// mutations.
	//
	// This method returns true if at least one row was returned. It
	// will also return an updated cursor that allows a caller to resume
	// reading.
	//
	// Rows will be emitted in (time, table, key) order. As such,
	// transaction boundaries can be detected by looking for a change in
	// the timestamp values.
	Unstage(ctx context.Context, tx StagingQuerier, q *UnstageCursor, fn UnstageCallback) (*UnstageCursor, bool, error)
}

// ToastedColumnPlaceholder is a placeholder to identify a unchanged
// Postgres toasted column. Must be quoted, so it can be used
// in JSON columns.
const ToastedColumnPlaceholder = `"__cdc__sink__toasted__"`

// ColData hold SQL column metadata.
type ColData struct {
	// A SQL expression to use with sparse payloads.
	DefaultExpr string
	Ignored     bool
	Name        ident.Ident
	// A Parse function may be supplied to allow a datatype
	// to be converted into a type more readily
	// used by a target database driver.
	Parse   func(any) (any, error) `json:"-"`
	Primary bool
	// Type of the column.
	Type string
}

// Equal returns true if the two ColData are equivalent under
// case-insensitivity.
func (d ColData) Equal(o ColData) bool {
	return d.DefaultExpr == o.DefaultExpr &&
		d.Ignored == o.Ignored &&
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
	ProductMariaDB
	ProductMySQL
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

	case ProductMySQL, ProductMariaDB, ProductOracle:
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

// TargetStatements is an injection point for a cache of prepared
// statements associated with the TargetPool.
type TargetStatements struct {
	*stmtcache.Cache[string]
}

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
	Get(db ident.Schema) (Watcher, error)
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
