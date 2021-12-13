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

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
)

func colSliceEqual(a, b []sinktypes.ColData) bool {
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
const sqlColumnsQuery = `
WITH
pks AS (
	SELECT column_name, seq_in_index FROM [SHOW INDEX FROM %[1]s]
	WHERE index_name = 'primary' AND NOT storing),
cols AS (
	SELECT column_name, data_type FROM [SHOW COLUMNS FROM %[1]s]
	WHERE generation_expression = ''),
ordered AS (
	SELECT column_name, min(ifnull(pks.seq_in_index, 2048)) AS seq_in_index FROM
	cols LEFT JOIN pks USING (column_name)
    GROUP BY column_name)
SELECT cols.column_name, pks.seq_in_index IS NOT NULL, cols.data_type
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
) ([]sinktypes.ColData, error) {
	stmt := fmt.Sprintf(sqlColumnsQuery, table)

	var columns []sinktypes.ColData
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.Query(ctx, stmt)
		if err != nil {
			return err
		}
		defer rows.Close()

		// Clear from previous loop.
		columns = columns[:0]
		for rows.Next() {
			var column sinktypes.ColData
			var name string
			if err := rows.Scan(&name, &column.Primary, &column.Type); err != nil {
				return err
			}
			column.Name = ident.New(name)
			columns = append(columns, column)
		}
		return nil
	})
	return columns, err
}
