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

// Package serial allows an otherwise-concurrent use of a database pool
// to be transparently deoptimized into serial use of a single
// transaction.
package serial

import (
	"context"
	"runtime"
	"sync"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

const (
	noTxMsg   = "no transaction in progress"
	txOpenMsg = "transaction already in progress"
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

var _ types.StagingQuerier = (*Pool)(nil)

// Begin opens a new transaction to concentrate work into.
func (s *Pool) Begin(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.tx != nil {
		return errors.New(txOpenMsg)
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
		return errors.New(noTxMsg)
	}
	s.mu.tx = nil
	return tx.Commit(ctx)
}

// Exec implements types.StagingQuerier and can only be called after Begin.
func (s *Pool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.mu.tx
	if tx == nil {
		return pgconn.NewCommandTag(""), errors.New(noTxMsg)
	}
	return tx.Exec(ctx, sql, arguments...)
}

// Query implements types.StagingQuerier and can only be called after Begin.
// The Rows that are returned from this method must be closed, fully
// consumed, or encounter an error before other query methods will be
// allowed to proceed.
func (s *Pool) Query(ctx context.Context, sql string, optionsAndArgs ...any) (pgx.Rows, error) {
	s.mu.Lock()
	// Unlocked by rowsUnlocker if no error.
	rows, err := s.queryLocked(ctx, sql, optionsAndArgs...)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	return rows, nil
}

// queryLocked either returns a rowsUnlocker or an error. Extracting
// this as a separate method makes the flow-control in Query() easier to
// read.
func (s *Pool) queryLocked(
	ctx context.Context, sql string, optionsAndArgs ...any,
) (*rowsUnlocker, error) {
	tx := s.mu.tx
	if tx == nil {
		return nil, errors.New(noTxMsg)
	}

	rows, err := tx.Query(ctx, sql, optionsAndArgs...)
	if err != nil {
		return nil, err

	}
	ret := &rowsUnlocker{rows, &s.mu}
	runtime.SetFinalizer(ret, func(r *rowsUnlocker) {
		r.unlock()
	})
	return ret, err
}

// QueryRow implements types.StagingQuerier and can only be called after
// Begin. The Row that is returned must be scanned before any other
// query methods wil be allowed to proceed.
func (s *Pool) QueryRow(ctx context.Context, sql string, optionsAndArgs ...any) pgx.Row {
	s.mu.Lock()
	// Unlocked by rowUnlocker.

	tx := s.mu.tx
	if tx == nil {
		return &rowUnlocker{
			err: errors.New(noTxMsg),
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
