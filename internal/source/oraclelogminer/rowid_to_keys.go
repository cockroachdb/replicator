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

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
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
)

// GetPKNames returns the primary key names of the given table.
func GetPKNames(
	ctx *stopper.Context, db *types.SourcePool, userName string, tableName string,
) ([]string, error) {
	q := fmt.Sprintf(getPKColNamesQuery, tableName, userName)
	pkNameRows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get the primary key of table %s", tableName)
	}

	pkNames := make([]string, 0)
	for pkNameRows.Next() {
		var pkName sql.NullString
		if err := pkNameRows.Scan(&pkName); err != nil {
			return nil, errors.Wrapf(err, "failed to scan for pk name for table %s", tableName)
		}
		if pkName.Valid {
			pkNames = append(pkNames, pkName.String)
		} else {
			return nil, errors.Errorf("cannot find valid pk name for table %s", tableName)
		}
	}

	return pkNames, nil
}
