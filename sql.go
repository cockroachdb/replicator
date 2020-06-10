// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
)

const sinkDBZoneConfig = `ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds = 600;`

// CreateSinkDB creates a new sink db if one does not exist yet and also adds
// the resolved table.
func CreateSinkDB(db *sql.DB) error {
	if err := Execute(db, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *sinkDB)); err != nil {
		return err
	}
	if *sinkDBZone {
		if err := Execute(db, fmt.Sprintf(sinkDBZoneConfig, *sinkDB)); err != nil {
			return err
		}
	}
	return CreateResolvedTable(db)
}

// DropSinkDB drops the sinkDB and all data in it.
func DropSinkDB(db *sql.DB) error {
	return Execute(db, fmt.Sprintf(`DROP DATABASE IF EXISTS %s CASCADE`, *sinkDB))
}

const sqlTableExistsQuery = `SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = '%s'`

// TableExists checks for the existence of a table.
func TableExists(db *sql.DB, dbName string, tableName string) (bool, error) {
	findTableSQL := fmt.Sprintf(sqlTableExistsQuery, dbName, tableName)
	var tableFound string
	err := crdb.Execute(func() error {
		return db.QueryRow(findTableSQL).Scan(&tableFound)
	})
	switch err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}

const sqlGetPrimaryKeyColumnsQuery = `
SELECT column_name FROM [SHOW INDEX FROM %s] WHERE index_name = 'primary' ORDER BY seq_in_index
`

// GetPrimaryKeyColumns returns the column names for the primary key index for
// a table, in order.
func GetPrimaryKeyColumns(db *sql.DB, tableFullName string) ([]string, error) {
	// Needs retry.
	findKeyColumns := fmt.Sprintf(sqlGetPrimaryKeyColumnsQuery, tableFullName)
	var columns []string
	if err := crdb.Execute(func() error {
		var columnsInternal []string
		rows, err := db.Query(findKeyColumns)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				return err
			}
			columnsInternal = append(columnsInternal, column)
		}
		columns = columnsInternal
		return nil
	}); err != nil {
		return nil, err
	}
	return columns, nil
}

// Execute is just a wrapper around crdb.Execute that can be used for sql
// queries that don't have any return values.
func Execute(db *sql.DB, query string, args ...interface{}) error {
	return crdb.Execute(func() error {
		_, err := db.Exec(query, args...)
		return err
	})
}
