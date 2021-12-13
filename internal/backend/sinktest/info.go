// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sinktest

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v4/pgxpool"
)

// DbInfo encapsulates metadata and a connection to a database.
type DbInfo struct {
	db      *pgxpool.Pool
	version string
}

// Pool returns the underlying database connection.
func (di DbInfo) Pool() *pgxpool.Pool { return di.db }

// Version returns the database version.
func (di DbInfo) Version() string { return di.version }

// TableInfo provides a named table and a means to access it.
type TableInfo struct {
	*DbInfo
	name ident.Table
}

// DeleteAll deletes (not TRUNCATEs) all rows in the table.
func (ti TableInfo) DeleteAll(ctx context.Context) error {
	return retry.Execute(ctx, ti.db, fmt.Sprintf("DELETE FROM %s WHERE true", ti.name))
}

// DropTable drops the table if it exists.
func (ti TableInfo) DropTable(ctx context.Context) error {
	return retry.Execute(ctx, ti.db, fmt.Sprintf("DROP TABLE IF EXISTS %s", ti.name))
}

// Name returns the table name.
func (ti TableInfo) Name() ident.Table { return ti.name }

// RowCount returns the number of rows in the table.
func (ti TableInfo) RowCount(ctx context.Context) (int, error) {
	return GetRowCount(ctx, ti.db, ti.Name())
}

func (ti TableInfo) String() string { return ti.name.String() }
