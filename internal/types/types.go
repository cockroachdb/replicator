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

// Package types contains data types and interfaces that define the major
// functional blocks of code within Replicator. The goal of placing the types
// into this package is to make it easy to compose functionality as the
// Replicator project evolves.
package types

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stmtcache"
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

// Leases coordinates behavior across multiple instances of Replicator.
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
	Before   json.RawMessage // Optional encoded JSON object
	Data     json.RawMessage // An encoded JSON object: { "key" : "hello" }
	Deletion bool            // Consider the mutation to be a deletion, even if Data is present.
	Key      json.RawMessage // Replication identity, an encoded JSON array: [ "hello" ]
	Meta     map[string]any  // Dialect-specific data, may be nil, not persisted
	Time     hlc.Time        // The effective time of the mutation
}

var nullBytes = []byte("null")

// HasData returns true if the Data field is populated.
func (m *Mutation) HasData() bool {
	switch len(m.Data) {
	case 0:
		return false
	case 4:
		return !bytes.Equal(m.Data, nullBytes)
	default:
		return true
	}
}

// IsDelete returns true if the Mutation represents a deletion.
func (m *Mutation) IsDelete() bool {
	return m.Deletion || len(m.Data) == 0 || bytes.Equal(m.Data, nullBytes)
}

// MutationFilter checks if a mutation satisfies a condition.
type MutationFilter func(Mutation) bool

// Stager describes a service which can durably persist some
// number of Mutations.
type Stager interface {
	// CheckConsistency scans the staging table to ensure that it
	// internally consistent. The number of inconsistent staging table
	// entries will be returned.
	CheckConsistency(ctx context.Context, db StagingQuerier, muts []Mutation, followerRead bool) (int, error)

	// FilterApplied performs an anti-join against the staging table to
	// return only unapplied mutations. This method will return a new
	// slice.
	FilterApplied(ctx context.Context, db StagingQuerier, muts []Mutation) ([]Mutation, error)

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

// StagingCursor is emitted by [Stagers.Read].
type StagingCursor struct {
	// A batch of data, corresponding to a transaction in the source
	// database. This may be nil for a progress-only update.
	Batch *TemporalBatch

	// This field will be populated if the reader encounters an
	// unrecoverable error while processing. A result containing an
	// error will be the final message in the channel before it is
	// closed.
	Error error

	// Fragment will be set if the Batch is not guaranteed to contain
	// all data for its given timestamp. This will occur, for example,
	// if the number of mutations for the timestamp exceeds
	// [StagingQuery.FragmentSize] or if an underlying database query is
	// not guaranteed to have yet read all values at the the batch's
	// time (e.g. scan boundary alignment). Consumers that require
	// transactionally-consistent views of the data should wait for the
	// next, non-fragmented, cursor update.
	Fragment bool

	// Jump indicates that the scanning bounds changed such that the
	// data in the stream may be disjoint.
	Jump bool

	// Progress indicates the range of data which has been successfully
	// scanned so far. Receivers may encounter progress-only updates
	// which happen when the end of the scanned bounds have been reached
	// or if there is a "pipeline bubble" when reading data from the
	// staging tables.
	Progress hlc.Range
}

// String is for debugging use only.
func (c *StagingCursor) String() string {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", " ")
	if err := enc.Encode(c); err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return buf.String()
}

// StagingQuery is passed to [Stagers.Read].
type StagingQuery struct {
	// The bounds variable governs the reader to ensure that it does not
	// read beyond the data known to be consistent, by pausing reads
	// when the maximum time has been reached. Updates to the minimum
	// bound allows records to be skipped, but a query cannot be rewound
	// such that it will re-emit records.
	Bounds *notify.Var[hlc.Range]

	// FragmentSize places an upper bound on the size of any individual
	// cursor entry. If a batch has exceeded the requested segment size,
	// the [StagingCursor.Fragment] flag will be set.
	FragmentSize int

	// The tables to query.
	Group *TableGroup
}

// Stagers is a factory for Stager instances.
type Stagers interface {
	Get(ctx context.Context, target ident.Table) (Stager, error)

	// Read provides access to joined staging data. Because this can
	// be a potentially expensive or otherwise unbounded amount of data,
	// the results are provided via a channel which may be incrementally
	// consumed from buffered data. Results will be provided in temporal
	// order.
	//
	// Any errors encountered while reading will be returned in the
	// final message before closing the channel.
	//
	// Care should be taken to [stopper.Context.Stop] the context passed
	// into this method to prevent goroutine or database leaks. When the
	// context is gracefully stopped, the channel will be closed
	// normally.
	Read(ctx *stopper.Context, q *StagingQuery) (<-chan *StagingCursor, error)
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

	// ErrCode returns a database error code, if err is of the pool's
	// underlying driver error type.
	ErrCode func(err error) (string, bool) `json:"-"`
	// HintNoFTS decorates the table name to prevent CockroachDB from
	// generating an UPSERT (or other) plan that may involve a full
	// table scan. For other databases or versions of CockroachDB that
	// do not support this hint, this function returns an unhinted table
	// name.
	//
	// https://www.cockroachlabs.com/docs/stable/table-expressions#prevent-full-scan
	// https://github.com/cockroachdb/cockroach/issues/98211
	HintNoFTS func(table ident.Table) *ident.Hinted[ident.Table] `json:"-"`
	// IsDeferrable returns true if the error might clear if the work is
	// tried at a future point in time (e.g.: foreign key dependencies).
	IsDeferrable func(err error) bool `json:"-"`
	// ShouldRetry returns true if the error should be retried
	// immediately (e.g.: serializable isolation failure).
	ShouldRetry func(err error) bool `json:"-"`
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
	// Watch returns an initialized variable that will be kept up to
	// date until the context is stopped or the table is dropped. If the
	// table is dropped, a zero-length slice will be emitted as a final
	// update. An error will be returned if the table is unknown.
	Watch(ctx *stopper.Context, table ident.Table) (*notify.Var[[]ColData], error)
}

// Watchers is a factory for Watcher instances.
type Watchers interface {
	Get(db ident.Schema) (Watcher, error)
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
