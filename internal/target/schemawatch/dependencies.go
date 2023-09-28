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
	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/pkg/errors"
)

// depOrderTemplateMySQL computes the "referential depth" of tables based on
// foreign-key constraints.
//
// The query is structured as follows:
//   - tables: Tables in the schema.
//   - refs: Maps referring tables (child) to referenced tables
//     (parent). Table self-references are excluded from this query.
//   - roots: Tables with no FK references.
//   - depths: Recursively computes the depth of each table from the root, walking
//     the foreign keys constraints.
//   - cycle_detect: insure that all tables have a depth, by injecting a default depth.
//     Finally, compute the maximum depth for each table in the given schema.

const depOrderTemplateMySQL = `
WITH RECURSIVE
  tables
    AS (
      SELECT
        table_catalog, table_schema, table_name
      FROM
        information_schema.tables
    ),
  refs
    AS (
        SELECT
            constraint_catalog AS child_catalog,
            constraint_schema AS child_schema,
            table_name AS child_table_name,
            unique_constraint_catalog AS parent_catalog,
            unique_constraint_schema AS parent_schema,
            referenced_table_name AS parent_table_name
            FROM information_schema.referential_constraints
       WHERE
        (constraint_catalog, constraint_schema, table_name)
        != (unique_constraint_catalog, unique_constraint_schema,referenced_table_name)
    ),
  roots
    AS (
      SELECT
        tables.table_catalog, tables.table_schema, tables.table_name
      FROM
        tables
      WHERE
        (tables.table_catalog, tables.table_schema, tables.table_name)
        NOT IN (SELECT child_catalog, child_schema, child_table_name FROM refs)
    ),
  depths
    AS (
      SELECT table_catalog, table_schema, table_name, 0 AS depth FROM roots
      UNION ALL
        SELECT
          refs.child_catalog,
          refs.child_schema,
          refs.child_table_name,
          depths.depth + 1
        FROM
          depths, refs
        WHERE
          refs.parent_catalog = depths.table_catalog
          AND refs.parent_schema = depths.table_schema
          AND refs.parent_table_name = depths.table_name
    ),
  cycle_detect
    AS (
      SELECT table_catalog, table_schema, table_name, -1 AS depth FROM tables
      UNION ALL
        SELECT table_catalog, table_schema, table_name, depth FROM depths
    )
SELECT
  table_name, max(depth) AS depth
FROM
  cycle_detect
WHERE
   table_schema = ?
GROUP BY
  table_name
ORDER BY
  depth, table_name`

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
SELECT tbl_name, max(lvl) - 1 lvl
FROM cyclic
WHERE tbl_owner = (:owner)
GROUP BY tbl_name
ORDER BY lvl, tbl_name`

// depOrderLegacyCRDB supports versions of CRDB <= v21.2 which experience
// an infinite loop when executing depOrderTemplatePg.
const depOrderLegacyCRDB = `
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
SELECT tbl, max(depth)
FROM (SELECT *, -1 AS depth FROM tables UNION ALL SELECT * FROM depths)
GROUP BY 1
ORDER BY 2, 1`

// depOrderTemplatePg computes the "referential depth" of tables based on
// foreign-key constraints. Note that this only works with acyclic FK
// dependency graphs. This is ok because CRDB's lack of deferrable
// constraints means that a cyclic dependency graph would be unusable.
// Once CRDB has deferrable constraints, the need for computing this
// dependency ordering goes away.
//
// The query is structured as follows:
//   - constraints: Used to resolve constraint names (i.e. primary or
//     unique indexes) to the table that defines them.
//   - tables: A list of all tables in the db.
//   - refs: Maps referring tables (child) to referenced tables
//     (parent). Table self-references are excluded from this query.
//   - roots: Tables that contain no FK references to ensure that
//     cyclical references remain unprocessed.
//   - depths: A recursive CTE that builds up from the roots. In each
//     step of the recursion, we select the child tables of the previous
//     iteration whose parent table has a known depth and use the maximum
//     parent's depth to derive the child's (updated) depth. The recursion
//     halts when the previous iteration contains only leaf tables.
//   - cycle_detect: Adds a sentinel depth value (-1) for all tables.
//   - The top-level query then finds the maximum depth for each table.
//     Any tables for which a depth cannot be computed (e.g. cyclical
//     references) will return the sentinel value from cycle_detect.
//
// One limitation in this query is that the information_schema doesn't
// appear to provide any way to know about the schema in which the
// referenced table is defined.
const depOrderTemplatePg = `
WITH RECURSIVE
  constraints
    AS (
      SELECT
        table_catalog, table_schema, table_name, constraint_name
      FROM
        %[1]s.information_schema.table_constraints
    ),
  tables
    AS (
      SELECT
        table_catalog, table_schema, table_name
      FROM
        %[1]s.information_schema.tables
    ),
  refs
    AS (
      SELECT
        ref.constraint_catalog AS child_catalog,
        ref.constraint_schema AS child_schema,
        child.table_name AS child_table_name,
        ref.unique_constraint_catalog AS parent_catalog,
        ref.unique_constraint_schema AS parent_schema,
        parent.table_name AS parent_table_name
      FROM
        %[1]s.information_schema.referential_constraints AS ref
        JOIN constraints AS child ON
            ref.constraint_catalog = child.table_catalog
            AND ref.constraint_schema = child.table_schema
            AND ref.constraint_name = child.constraint_name
        JOIN constraints AS parent ON
            ref.unique_constraint_catalog = parent.table_catalog
            AND ref.unique_constraint_schema = parent.table_schema
            AND ref.unique_constraint_name = parent.constraint_name
      WHERE
        (child.table_catalog, child.table_catalog, child.table_name)
        != (parent.table_catalog, parent.table_catalog, parent.table_name)
    ),
  roots
    AS (
      SELECT
        tables.table_catalog, tables.table_schema, tables.table_name
      FROM
        tables
      WHERE
        (tables.table_catalog, tables.table_schema, tables.table_name)
        NOT IN (SELECT child_catalog, child_schema, child_table_name FROM refs)
    ),
  depths
    AS (
      SELECT table_catalog, table_schema, table_name, 0 AS depth FROM roots
      UNION ALL
        SELECT
          refs.child_catalog,
          refs.child_schema,
          refs.child_table_name,
          depths.depth + 1
        FROM
          depths, refs
        WHERE
          refs.parent_catalog = depths.table_catalog
          AND refs.parent_schema = depths.table_schema
          AND refs.parent_table_name = depths.table_name
    ),
  cycle_detect
    AS (
      SELECT table_catalog, table_schema, table_name, -1 AS depth FROM tables
      UNION ALL
        SELECT table_catalog, table_schema, table_name, depth FROM depths
    )
SELECT
  table_name, max(depth) AS depth
FROM
  cycle_detect
WHERE
  table_catalog = $1 AND table_schema = $2
GROUP BY
  table_name
ORDER BY
  depth, table_name`

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
		parts := db.Idents(make([]ident.Ident, 0, 2))
		if len(parts) != 2 {
			return nil, errors.Errorf("expecting two schema parts, had %d", len(parts))
		}

		if tx.Product == types.ProductCockroachDB && strings.Contains(tx.Version, "v21.") {
			stmt = fmt.Sprintf(depOrderLegacyCRDB, db, parts[0])
		} else {
			stmt = fmt.Sprintf(depOrderTemplatePg, parts[0])
			args = []any{parts[0].Raw(), parts[1].Raw()}
		}

	case types.ProductMySQL:
		parts := db.Idents(make([]ident.Ident, 0, 1))
		if len(parts) != 1 {
			return nil, errors.Errorf("expecting one schema parts, had %d", len(parts))
		}
		stmt = depOrderTemplateMySQL
		args = []any{parts[0].Raw()}

	case types.ProductOracle:
		stmt = depOrderTemplateOra
		args = []any{sql.Named("owner", db.Raw())}
	default:
		return nil, errors.Errorf("getDependencyOrder unimplemented product: %s", tx.Product)
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
			var tableName string
			var nextOrder int

			if err := rows.Scan(&tableName, &nextOrder); err != nil {
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
