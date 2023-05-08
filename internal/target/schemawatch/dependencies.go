// Copyright 2022 The Cockroach Authors.
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
	"github.com/pkg/errors"
)

// depOrderTemplate computes the "referential depth" of tables based on
// foreign-key constraints. Note that this only works with acyclic FK
// dependency graphs. This is ok because CRDB's lack of deferrable
// constraints means that a cyclic dependency graph would be unusable.
// Once CRDB has deferrable constraints, the need for computing this
// dependency ordering goes away.
//
// The query is structured as follows:
//   - tables: A list of (schema, table) pairs for all tables in the db.
//   - refs: Maps referring tables (child) to referenced tables
//     (parent). Table self-references are excluded from this query.
//   - roots: Tables that contain no FK references to ensure that
//     cyclical references remain unprocessed.
//   - depths: A recursive CTE that builds up from the roots. In each
//     step of the recursion, we select the child tables of the previous
//     iteration whose parent table has a known depth and use the maximum
//     parent's depth to derive the child's (updated) depth. The recursion
//     halts when the previous iteration contains only leaf tables.
//   - The top-level query then finds the maximum depth for each table.
//     Any tables for which a depth cannot be computed (e.g. cyclical
//     references) are assigned a sentinel value.
//
// One limitation in this query is that the information_schema doesn't
// appear to provide any way to know about the schema in which the
// referenced table is defined.
const depOrderTemplate = `
WITH RECURSIVE
 tables AS (
   SELECT schema_name AS sch, table_name AS tbl
   FROM [SHOW TABLES FROM %[1]s]),
 refs AS (
   SELECT
    constraint_schema AS child_sch, table_name AS child_tbl, referenced_table_name AS parent_tbl
   FROM %[1]s.information_schema.referential_constraints
   WHERE table_name != referenced_table_name
 ),
 roots AS (
   SELECT tables.sch, tables.tbl, 0 AS depth
   FROM tables
   WHERE (tables.sch, tables.tbl) NOT IN (SELECT (child_sch, child_tbl) FROM refs) 
 ),
 depths AS (
   SELECT * FROM roots
   UNION ALL
    SELECT refs.child_sch, refs.child_tbl, max(depths.depth) + 1
    FROM depths, refs
    WHERE refs.parent_tbl = depths.tbl
    GROUP BY 1, 2
 )
SELECT sch, tbl, max(depth)
FROM (SELECT *, -1 AS depth FROM tables UNION ALL SELECT * FROM depths)
GROUP BY 1, 2
ORDER BY 3, 1, 2
`

// getDependencyOrder returns equivalency groups of tables defined
// within the given database. The order of the slice will satisfy
// the (acyclic) foreign-key dependency graph.
func getDependencyOrder(
	ctx context.Context, tx types.Querier, db ident.Ident,
) ([][]ident.Table, error) {
	stmt := fmt.Sprintf(depOrderTemplate, db)

	var cycles []ident.Table
	var depOrder [][]ident.Table
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.Query(ctx, stmt)
		if err != nil {
			return err
		}
		defer rows.Close()

		currentOrder := -1
		for rows.Next() {
			var schemaName, tableName string
			var nextOrder int

			if err := rows.Scan(&schemaName, &tableName, &nextOrder); err != nil {
				return err
			}

			tbl := ident.NewTable(db, ident.New(schemaName), ident.New(tableName))

			// Table has no well-defined ordering.
			if nextOrder < 0 {
				cycles = append(cycles, tbl)
				continue
			}

			if nextOrder > currentOrder {
				currentOrder = nextOrder
				depOrder = append(depOrder, nil)
			}
			depOrder[currentOrder] = append(depOrder[currentOrder], tbl)
		}
		return nil
	})

	if len(cycles) > 0 {
		return nil, errors.Errorf("cyclical FK references involving tables %s", cycles)
	}

	return depOrder, err
}
