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
	"math/rand"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// TableInfo provides a named table and a means to access it.
// Instances are created via CreateTable.
type TableInfo struct {
	*DBInfo
	name ident.Table
}

// CreateTable creates a test table and returns a unique name. The
// schemaSpec parameter must have exactly one %s substitution parameter
// for the database name and table name.
func CreateTable(ctx context.Context, dbName ident.Ident, schemaSpec string) (TableInfo, error) {
	var table ident.Table
	db := DB(ctx)
	if db == nil {
		return TableInfo{}, errors.New("no database in context")
	}

outer:
	for {
		// Create the testing database
		tableNum := rand.Intn(10000)
		tableName := ident.New(fmt.Sprintf("_test_table_%d", tableNum))

		// Find the DB.
		var actualTableName string
		err := retry.Retry(ctx, func(ctx context.Context) error {
			return db.Pool().QueryRow(ctx,
				fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = $1", dbName),
				tableName.Raw(),
			).Scan(&actualTableName)
		})
		switch err {
		case pgx.ErrNoRows:
			table = ident.NewTable(dbName, ident.Public, tableName)
			break outer
		case nil:
			continue
		default:
			return TableInfo{}, errors.WithStack(err)
		}
	}

	err := retry.Execute(ctx, db.Pool(), fmt.Sprintf(schemaSpec, table))
	return TableInfo{db, table}, errors.WithStack(err)
}

// NewTableInfo constructs a TableInfo using the given name.
func NewTableInfo(db *DBInfo, name ident.Table) TableInfo {
	return TableInfo{db, name}
}

// DeleteAll deletes (not TRUNCATEs) all rows in the table.
func (ti TableInfo) DeleteAll(ctx context.Context) error {
	return retry.Execute(ctx, ti.db, fmt.Sprintf("DELETE FROM %s WHERE true", ti.name))
}

// DropTable drops the table if it exists.
func (ti TableInfo) DropTable(ctx context.Context) error {
	return retry.Execute(ctx, ti.db, fmt.Sprintf("DROP TABLE IF EXISTS %s", ti.name))
}

// Exec executes a single SQL statement. The sql string must include
// a single string substitution marker to receive the table name.
func (ti TableInfo) Exec(ctx context.Context, sql string, args ...interface{}) error {
	return retry.Execute(ctx, ti.Pool(), fmt.Sprintf(sql, ti.Name()), args...)
}

// Name returns the table name.
func (ti TableInfo) Name() ident.Table { return ti.name }

// RowCount returns the number of rows in the table.
func (ti TableInfo) RowCount(ctx context.Context) (int, error) {
	return GetRowCount(ctx, ti.db, ti.Name())
}

func (ti TableInfo) String() string { return ti.name.String() }
