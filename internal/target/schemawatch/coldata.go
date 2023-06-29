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

package schemawatch

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/pkg/errors"
)

func colSliceEqual(a, b []types.ColData) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Retrieve the primary key columns in their index-order, then append
// any remaining non-generated columns.
//
// Parts of the CTE:
// * pk_name: finds the name of the primary key constraint for the table
// * pks: extracts the names of the PK columns and their relative
// positions. We exclude any "storing" columns to account for rowid
// value.
// * cols: extracts all columns, ignoring those with generation
// expressions (e.g. hash-sharded index clustering column).
// * ordered: adds a synthetic seq_in_index to the non-PK columns.
// * SELECT: aggregates the above, sorting the PK columns in-order
// before the non-PK columns.
const sqlColumnsQuery = `
WITH
pk_name AS (
	SELECT constraint_name FROM [SHOW CONSTRAINTS FROM %[1]s]
	WHERE constraint_type = 'PRIMARY KEY'),
pks AS (
	SELECT column_name, seq_in_index FROM [SHOW INDEX FROM %[1]s]
	JOIN pk_name ON (index_name = constraint_name)
	WHERE NOT storing),
cols AS (
	SELECT column_name, data_type, generation_expression != '' AS ignored
	FROM [SHOW COLUMNS FROM %[1]s]),
ordered AS (
	SELECT column_name, min(ifnull(pks.seq_in_index, 2048)) AS seq_in_index FROM
	cols LEFT JOIN pks USING (column_name)
    GROUP BY column_name)
SELECT cols.column_name, pks.seq_in_index IS NOT NULL, cols.data_type, cols.ignored
FROM cols
JOIN ordered USING (column_name)
LEFT JOIN pks USING (column_name)
ORDER BY ordered.seq_in_index, cols.column_name
`

// getColumns returns the column names for the primary key columns in
// their index-order, followed by all other columns that should be
// mutated.
func getColumns(
	ctx context.Context, tx *types.TargetPool, table ident.Table,
) ([]types.ColData, error) {
	stmt := fmt.Sprintf(sqlColumnsQuery, table)

	var columns []types.ColData
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.QueryContext(ctx, stmt)
		if err != nil {
			return err
		}
		defer rows.Close()

		// Clear from previous loop.
		columns = columns[:0]
		foundPrimay := false
		for rows.Next() {
			var column types.ColData
			var rawColType, name string
			if err := rows.Scan(&name, &column.Primary, &rawColType, &column.Ignored); err != nil {
				return err
			}
			column.Name = ident.New(name)
			if column.Primary {
				foundPrimay = true
			}

			// If the column type is a user-defined type, e.g. an enum,
			// then we want to treat it as a proper database ident, and
			// not a string.  Fortunately, UDTs can be identified
			// because they have (at least) the schema name reported by
			// the introspection query.  We can't blindly treat all type
			// names as idents, because the introspection query returns
			// upcased value (e.g. INT8), while the actual type name of
			// builtin types are lower case (e.g. int8). Thus, the SQL
			// expression 1::"INT8" is invalid, although 1::"int8" would
			// be fine. Baking in a list of intrinsic datatypes also
			// seems somewhat brittle as CRDB evolves.
			parts := make([]ident.Ident, 0, 3)
			for remainder := rawColType; remainder != ""; {
				// Strip leading . from previous loop.
				if remainder[0] == '.' {
					remainder = remainder[1:]
				}
				var part ident.Ident
				var err error
				part, remainder, err = ident.ParseIdent(remainder)
				if err != nil {
					return err
				}
				parts = append(parts, part)
			}
			switch len(parts) {
			case 1:
				// Raw type, like INT8
				column.Type = parts[0].Raw()
			case 2:
				// UDT for CRDB <= 22.1 only includes schema and name.
				relSchema, _, err := table.Schema().Relative(parts[0])
				if err != nil {
					return err
				}
				column.Type = ident.NewUDT(relSchema, parts[1])
			case 3:
				// Fully-qualified UDT.
				relSchema, _, err := table.Schema().Relative(parts[0], parts[1])
				if err != nil {
					return err
				}
				column.Type = ident.NewUDT(relSchema, parts[2])
			default:
				return errors.Errorf("unexpected type name %q", rawColType)
			}

			columns = append(columns, column)
		}

		// It's legal, if unusual, to create a table with no columns.
		if len(columns) == 0 {
			columns = []types.ColData{
				{
					Ignored: false,
					Name:    ident.New("rowid"),
					Primary: true,
					Type:    "INT8",
				},
			}
			return nil
		}

		// If there are no primary key columns, we know that a synthetic
		// rowid column will exist. We'll create a new slice which
		// respects the ordering guarantees.
		if !foundPrimay {
			rowID := ident.New("rowid")

			next := make([]types.ColData, len(columns))
			next[0] = types.ColData{
				Ignored: false,
				Name:    rowID,
				Primary: true,
				Type:    "INT8",
			}

			nextIdx := 1
			for _, col := range columns {
				if col.Name != rowID {
					next[nextIdx] = col
					nextIdx++
				}
			}
			columns = next
		}

		return nil
	})
	return columns, err
}
