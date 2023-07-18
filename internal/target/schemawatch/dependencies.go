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

// depOrderTemplateOra computes the "referential depth" of tables based on
// foreign-key constraints.
//
// The query is structured as follows:
//   - parent_refs: Identifies all index References as a mapping of
//     (owner, table) -> (owner, constraint).
//   - ref_to_tbl: Resolves the constraint name references to Primary or
//     Unique indexes to parent table names.
//   - tbl_to_parent: Joins parent_refs and ref_to_tbl to produce
//     child(owner, table) -> parent(owner, table) mappings.
//   - root: Any table that is not referenced in tbl_to_parent. Contains
//     extra columns to be column-compatible with tbl_to_parent.
//   - combo: A union of root and tbl_to_parent. This serves as the
//     source of data for the recursive query.
//   - levels: A recursive query that computes the level by finding the
//     child tables of the previous level. The START WITH clause selects
//     tables which have no parent or which are self-referential.
//   - cyclic: Adds a dummy level for every table to detect any cyclic
//     structures which were excluded by levels.
//   - The top-level query finds the maximum depth for each table.  We
//     subtract one from the magic LEVEL value to align with the PG query
//     below.
const depOrderTemplateOra = `
WITH parent_refs AS (SELECT OWNER tbl_owner, TABLE_NAME tbl_name, R_OWNER parent_owner, R_CONSTRAINT_NAME ref_name
                     FROM ALL_CONSTRAINTS
                     WHERE CONSTRAINT_TYPE = 'R'),
     ref_to_tbl AS (SELECT CONSTRAINT_NAME ref_name, OWNER parent_owner, TABLE_NAME parent_name
                    FROM ALL_CONSTRAINTS
                    WHERE CONSTRAINT_TYPE IN ('P', 'U')),
     tbl_to_parent AS (SELECT tbl_owner, tbl_name, parent_owner, parent_name
                       FROM parent_refs
                                JOIN ref_to_tbl USING (parent_owner, ref_name)),
     roots AS (SELECT OWNER tbl_owner, TABLE_NAME tbl_name, NULL parent_owner, NULL parent_name
               FROM ALL_TABLES
               WHERE (OWNER, TABLE_NAME) NOT IN (SELECT tbl_owner, tbl_name FROM tbl_to_parent)),
     combo AS (SELECT * FROM roots UNION ALL SELECT * from tbl_to_parent),
     levels AS (SELECT LEVEL lvl, tbl_owner, tbl_name, parent_owner, parent_name
                FROM combo
                START WITH (parent_owner IS NULL AND parent_name IS NULL)
                        OR (tbl_owner = parent_owner AND tbl_name = parent_name)
                CONNECT BY NOCYCLE (parent_owner, parent_name) = ((PRIOR tbl_owner, PRIOR tbl_name))),
     cyclic AS (SELECT 0 lvl, OWNER tbl_owner, TABLE_NAME tbl_name
                FROM ALL_TABLES
                UNION ALL
                SELECT lvl, tbl_owner, tbl_name
                FROM levels)
SELECT lower(tbl_owner), lower(tbl_name), max(lvl) - 1 lvl
FROM cyclic
WHERE tbl_owner = :owner
GROUP BY tbl_owner, tbl_name
ORDER BY 3, 1, 2`

// depOrderTemplatePg computes the "referential depth" of tables based on
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
const depOrderTemplatePg = `
WITH RECURSIVE
 tables AS (
   SELECT schema_name AS sch, table_name AS tbl
   FROM [SHOW TABLES FROM %[1]s]),
 refs AS (
   SELECT
    constraint_schema AS child_sch, table_name AS child_tbl, referenced_table_name AS parent_tbl
   FROM %[2]s.information_schema.referential_constraints
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
	ctx context.Context, tx *types.TargetPool, db ident.Schema,
) ([][]ident.Table, error) {
	var args []any
	var stmt string
	switch tx.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		// Extract just the database name to refer to information_schema.
		dbName, _ := db.Split()
		stmt = fmt.Sprintf(depOrderTemplatePg, db, dbName)
	case types.ProductOracle:
		stmt = depOrderTemplateOra
		args = []any{db.Raw()}
	}

	var cycles []ident.Table
	var depOrder [][]ident.Table
	err := retry.Retry(ctx, func(ctx context.Context) error {
		rows, err := tx.QueryContext(ctx, stmt, args...)
		if err != nil {
			return errors.Wrap(err, stmt)
		}
		defer rows.Close()

		currentOrder := -1
		for rows.Next() {
			var schemaName, tableName string
			var nextOrder int

			if err := rows.Scan(&schemaName, &tableName, &nextOrder); err != nil {
				return err
			}

			tbl := ident.NewTable(db, ident.New(tableName))

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
