// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemawatch

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
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
	ctx context.Context, tx pgxtype.Querier, table ident.Table,
) ([]types.ColData, error) {
	stmt := fmt.Sprintf(sqlColumnsQuery, table)

	var columns []types.ColData
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.Query(ctx, stmt)
		if err != nil {
			return err
		}
		defer rows.Close()

		// Clear from previous loop.
		columns = columns[:0]
		foundPrimay := false
		for rows.Next() {
			var column types.ColData
			var name string
			if err := rows.Scan(&name, &column.Primary, &column.Type, &column.Ignored); err != nil {
				return err
			}
			column.Name = ident.New(name)
			if column.Primary {
				foundPrimay = true
			}
			columns = append(columns, column)
		}

		// If there are no primary key columns, we know that a synthetic
		// rowid column will exist. We'll prepend it to the slice and
		// then delete the
		if !foundPrimay {
			rowID := types.ColData{
				Ignored: false,
				Name:    ident.New("rowid"),
				Primary: true,
				Type:    "INT8",
			}
			// Filter and prepend.
			curIdx := 0
			for _, col := range columns {
				if col.Name != rowID.Name {
					columns[curIdx] = col
					curIdx++
				}
			}
			columns = append([]types.ColData{rowID}, columns[:curIdx]...)
		}

		return nil
	})
	return columns, err
}
