// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package serial allows an otherwise-concurrent use of a database pool
// to be transparently deoptimized into serial use of a single
// transaction.
package serial

import (
	"context"
	"runtime"
	"sync"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// Pool is a wrapper around a database connection pool that
// concentrates database queries into a single transaction. This type
// is used when it is necessary to de-optimize concurrent database
// performance in favor of transactional consistency.
//
// This type is internally synchronized, and the query method calls will
// block each other. Queries will also be blocked while there is an
// active pgx.Rows or un-scanned pgx.Row that has been returned from a
// query.
type Pool struct {
	Pool *pgxpool.Pool

	mu struct {
		sync.Mutex
		tx pgx.Tx
	}
}

var _ pgxtype.Querier = (*Pool)(nil)

// Begin opens a new transaction to concentrate work into.
func (s *Pool) Begin(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.tx != nil {
		return errors.New("transaction already in progress")
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	s.mu.tx = tx
	return nil
}

// Commit commits the underlying transaction.
func (s *Pool) Commit(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.mu.tx
	if tx == nil {
		return errors.New("no transaction in progress")
	}
	s.mu.tx = nil
	return tx.Commit(ctx)
}

// Rollback abort the underlying transaction, if one is present.
// This method is always safe to call.
func (s *Pool) Rollback(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.mu.tx
	if tx == nil {
		return nil
	}
	s.mu.tx = nil
	return tx.Rollback(ctx)
}

// Exec implements pgxtype.Querier and can only be called after Begin.
func (s *Pool) Exec(
	ctx context.Context, sql string, arguments ...interface{},
) (pgconn.CommandTag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.mu.tx
	if tx == nil {
		return nil, errors.New("no transaction in progress")
	}
	return tx.Exec(ctx, sql, arguments...)
}

// Query implements pgxtype.Querier and can only be called after Begin.
// The Rows that are returned from this method must be closed, fully
// consumed, or encounter an error before other query methods will be
// allowed to proceed.
func (s *Pool) Query(
	ctx context.Context, sql string, optionsAndArgs ...interface{},
) (pgx.Rows, error) {
	s.mu.Lock()

	tx := s.mu.tx
	if tx == nil {
		s.mu.Unlock()
		return nil, errors.New("no transaction in progress")
	}

	rows, err := tx.Query(ctx, sql, optionsAndArgs...)
	if err == nil {
		rows = &rowsUnlocker{rows, &s.mu}
		runtime.SetFinalizer(rows, func(r *rowsUnlocker) {
			r.unlock()
		})
	} else {
		s.mu.Unlock()
	}
	return rows, err
}

// QueryRow implements pgxtype.Querier and can only be called after
// Begin. The Row that is returned must be scanned before any other
// query methods wil be allowed to proceed.
func (s *Pool) QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) pgx.Row {
	s.mu.Lock()

	tx := s.mu.tx
	if tx == nil {
		return &rowUnlocker{
			err: errors.New("no transaction in progress"),
			u:   &s.mu,
		}
	}
	row := &rowUnlocker{
		r: tx.QueryRow(ctx, sql, optionsAndArgs...),
		u: &s.mu,
	}
	runtime.SetFinalizer(row, func(row *rowUnlocker) {
		row.unlock()
	})
	return row
}

// rowUnlocker calls an Unlock method once it has been scanned.
// It can also be configured to always return a specific error.
type rowUnlocker struct {
	err error
	r   pgx.Row
	u   interface{ Unlock() }
}

var _ pgx.Row = (*rowUnlocker)(nil)

func (r *rowUnlocker) Scan(dest ...interface{}) error {
	defer r.unlock()
	var err error
	if r.err == nil {
		err = r.r.Scan(dest...)
	} else {
		err = r.err
	}
	return err
}

func (r *rowUnlocker) unlock() {
	if r.u != nil {
		runtime.SetFinalizer(r, nil)
		r.u.Unlock()
		r.u = nil
	}
}

// rowsUnlocker will call an Unlock method once the Rows has been
// closed, exhausted, or enters an error state.
type rowsUnlocker struct {
	r pgx.Rows
	u interface{ Unlock() }
}

var _ pgx.Rows = (*rowsUnlocker)(nil)

func (r *rowsUnlocker) CommandTag() pgconn.CommandTag {
	return r.r.CommandTag()
}

func (r *rowsUnlocker) Close() {
	r.r.Close()
	r.unlock()
}

func (r *rowsUnlocker) Err() error {
	err := r.r.Err()
	if err != nil {
		r.unlock()
	}
	return err
}

func (r *rowsUnlocker) FieldDescriptions() []pgproto3.FieldDescription {
	return r.r.FieldDescriptions()
}

func (r *rowsUnlocker) Next() bool {
	if r.r.Next() {
		return true
	}
	r.unlock()
	return false
}

func (r *rowsUnlocker) RawValues() [][]byte {
	return r.r.RawValues()
}

func (r *rowsUnlocker) Scan(dest ...interface{}) error {
	err := r.r.Scan(dest...)
	if err != nil {
		r.unlock()
	}
	return err
}

func (r *rowsUnlocker) Values() ([]interface{}, error) {
	ret, err := r.r.Values()
	if err != nil {
		r.unlock()
	}
	return ret, err
}

func (r *rowsUnlocker) unlock() {
	if u := r.u; u != nil {
		runtime.SetFinalizer(r, nil)
		u.Unlock()
		r.u = nil
	}
}
