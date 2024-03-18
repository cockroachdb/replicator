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

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
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
