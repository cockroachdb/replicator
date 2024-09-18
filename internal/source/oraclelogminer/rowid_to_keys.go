// Copyright 2024 The Cockroach Authors
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

package oraclelogminer

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/jackc/pgx/v5"
)

const (
	getPKColNamesQuery = `
SELECT column_name
FROM all_cons_columns
WHERE constraint_name = (
    SELECT constraint_name
    FROM all_constraints
    WHERE table_name = '%s'
      AND constraint_type = 'P' AND OWNER = '%s'
)
`

	getPKValuesForRowIDQuery = `
SELECT %s FROM %s.%s AS OF SCN %s WHERE ROWID = '%s'
`
)

// RowIDToPKs converts a rowID to the corresponding primary key values at the given SCN.
// This is needed for UPDATE changefeed, where the PK values are not specified in the sql stmt.
func RowIDToPKs(
	ctx *stopper.Context, db *sql.DB, rowID string, userName string, tableName string, SCN string,
) ([]string, [][]byte, error) {
	if rowID == "" {
		return nil, nil, errors.AssertionFailedf("row id is empty")
	}

	//  Get the name of all PK columns for the table.
	// TODO(janexing): we might not want to query for the pk col names for every changefeed.
	// Consider moving this to a prior step where we determine the schema of the table.
	pkNameRows, err := db.QueryContext(ctx, fmt.Sprintf(getPKColNamesQuery, tableName, userName))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get the primary key of table %s", tableName)
	}

	pkNames := make([]string, 0)
	for pkNameRows.Next() {
		var pkName sql.NullString
		if err := pkNameRows.Scan(&pkName); err != nil {
			return nil, nil, errors.Wrapf(err, "failed to scan for pk name for table %s", tableName)
		}
		if pkName.Valid {
			pkNames = append(pkNames, pkName.String)
		} else {
			return nil, nil, errors.Newf("cannot find valid pk name for table %s", tableName)
		}
	}

	// Prepare a slice of interfaces to hold column values.
	values := make([]interface{}, len(pkNames))
	byteValues := make([][]byte, len(pkNames))

	// Fill values slice with pointers to byte slices.
	for i := range values {
		values[i] = &byteValues[i]
	}

	query := fmt.Sprintf(getPKValuesForRowIDQuery, strings.Join(pkNames, ","), userName, tableName, SCN, rowID)
	if err := db.QueryRowContext(ctx, query).Scan(values...); err != nil && errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, errors.Wrapf(err, "failed to scan for the primary key value of row id %s for table %s", rowID, tableName)
	}

	// It can happen that the pk value is in fact nil if this is an insert stmt.
	if byteValues[0] == nil {
		return pkNames, nil, nil
	}

	return pkNames, byteValues, nil
}
