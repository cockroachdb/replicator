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

package base

import (
	"context"
	"fmt"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
)

// TableInfo provides a named table and a means to access it.
// Instances are created via CreateTable.
type TableInfo[P types.AnyPool] struct {
	db   P
	name ident.Table
}

// NewTableInfo constructs a TableInfo using the given name.
func NewTableInfo[P types.AnyPool](db P, name ident.Table) TableInfo[P] {
	return TableInfo[P]{db, name}
}

// DeleteAll deletes (not TRUNCATEs) all rows in the table.
func (ti TableInfo[P]) DeleteAll(ctx context.Context) error {
	return retry.Execute(ctx, ti.db, fmt.Sprintf("DELETE FROM %s WHERE true", ti.name))
}

// DropTable drops the table.
func (ti TableInfo[P]) DropTable(ctx context.Context) error {
	return retry.Execute(ctx, ti.db, fmt.Sprintf("DROP TABLE %s", ti.name))
}

// Exec executes a single SQL statement. The sql string must include
// a single string substitution marker to receive the table name.
func (ti TableInfo[P]) Exec(ctx context.Context, sql string, args ...any) error {
	return retry.Execute(ctx, ti.db, fmt.Sprintf(sql, ti.Name()), args...)
}

// Name returns the table name.
func (ti TableInfo[P]) Name() ident.Table { return ti.name }

// RowCount returns the number of rows in the table.
func (ti TableInfo[P]) RowCount(ctx context.Context) (int, error) {
	return GetRowCount(ctx, ti.db, ti.Name())
}

func (ti TableInfo[P]) String() string { return ti.name.String() }

// GetRowCount returns the number of rows in the table.
func GetRowCount[P types.AnyPool](ctx context.Context, db P, name ident.Table) (int, error) {
	var count int
	err := retry.Retry(ctx, db, func(ctx context.Context) error {
		switch t := any(db).(type) {
		case *types.SourcePool:
			return t.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&count)
		case *types.StagingPool:
			return t.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&count)
		case *types.TargetPool:
			return t.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&count)
		default:
			return errors.Errorf("unimplemented %T", t)
		}
	})
	return count, err
}

// GetRowCountWithPredicate returns the number of rows in the table that match the predicate.
// Technically this is not a predicate match but rather checking that V is equal to the search string.
// The more I look at this solution I'm not that happy with it. The Flex option below is better.
func GetRowCountWithPredicate[P types.AnyPool](ctx context.Context, db P, name ident.Table, product types.Product, predicate string) (int, error) {
	// Handles the query strings for various product types.
	var q string
	switch product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		// TODO: open question: should the "v" be another parameter that
		// we fmt.Sprintf in? Technically we want this flexible so folks can
		// use it for other values.
		// Alternatively, we can consider just passing in the whole predicate:
		// for example: "v = 'cowbell'". or num > 10.
		// There will be work here to make more flexible.
		q = "SELECT count(*) FROM %s WHERE v = $1"
	case types.ProductMariaDB, types.ProductMySQL:
		q = "SELECT count(*) FROM %s WHERE v = ?"
	case types.ProductOracle:
		q = "SELECT count(*) FROM %s WHERE v = :v"
	default:
		return 0, errors.New("unimplemented product")
	}
	query := fmt.Sprintf(q, name)

	// Handles the per pool specifics.
	var count int
	err := retry.Retry(ctx, db, func(ctx context.Context) error {
		switch t := any(db).(type) {
		case *types.SourcePool:
			return t.QueryRowContext(ctx, query, predicate).Scan(&count)
		case *types.StagingPool:
			return t.QueryRow(ctx, query, predicate).Scan(&count)
		case *types.TargetPool:
			return t.QueryRowContext(ctx, query, predicate).Scan(&count)
		default:
			return errors.Errorf("unimplemented %T", t)
		}
	})
	return count, err
}

// GetRowCountWithPredicateFlex returns the number of rows in the table that match the predicate.
// This method supports N number of predicates for filtering the table.
// The downside of this method is that escaping and sanitising the input is an exercise left to the caller.
func GetRowCountWithPredicateFlex[P types.AnyPool](ctx context.Context, db P, name ident.Table, product types.Product, predicate string) (int, error) {
	// Handles the query strings for various product types.
	var q string
	switch product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		q = "SELECT count(*) FROM %s WHERE %s"
	case types.ProductMariaDB, types.ProductMySQL:
		q = "SELECT count(*) FROM %s WHERE %s"
	case types.ProductOracle:
		q = "SELECT count(*) FROM %s WHERE %s"
	default:
		return 0, errors.New("unimplemented product")
	}
	query := fmt.Sprintf(q, name, predicate)

	// Handles the per pool specifics.
	var count int
	err := retry.Retry(ctx, db, func(ctx context.Context) error {
		switch t := any(db).(type) {
		case *types.SourcePool:
			return t.QueryRowContext(ctx, query).Scan(&count)
		case *types.StagingPool:
			return t.QueryRow(ctx, query).Scan(&count)
		case *types.TargetPool:
			return t.QueryRowContext(ctx, query).Scan(&count)
		default:
			return errors.Errorf("unimplemented %T", t)
		}
	})
	return count, err
}
