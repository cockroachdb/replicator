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

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// depOrderTemplateMySQL creates a mapping of child to parent tables.
//
// CTE elements:
//   - q: The catalog and schema to query
//   - tables: All base tables in the target schema
//   - refs: Creates child to parent mappings. In information_schema, an
//     FK reference is expressed as a constraint applied to some (child)
//     table that references a unique constraint on another (parent)
//     table.
//   - seeds: Emits a dummy row for all tables in the schema. This
//     ensures that tables with no children are still in the result set.
//   - top level: Union of refs and seeds
const depOrderTemplateMySQL = `
WITH
 q (table_catalog, table_schema) AS (SELECT ?, ?),
 tables
  AS (
   SELECT
    table_catalog, table_schema, table_name
   FROM
    information_schema.tables JOIN q USING (table_catalog, table_schema)
   WHERE
    table_type = 'BASE TABLE'
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
   FROM
    information_schema.referential_constraints AS r
    JOIN q ON
      r.unique_constraint_catalog = q.table_catalog
      AND (
        r.unique_constraint_schema = q.table_schema
        OR r.constraint_schema = q.table_schema
       )
   WHERE
    (constraint_schema, table_name) != (unique_constraint_schema, referenced_table_name)
  ),
 seeds
  AS (
   SELECT
    NULL AS child_catalog,
    NULL AS child_schema,
    NULL AS child_table_name,
    table_catalog AS parent_catalog,
    table_schema AS parent_schema,
    table_name AS parent_table_name
   FROM
    tables
  )
SELECT * FROM seeds
UNION ALL
SELECT * FROM refs
`

// depOrderTemplateOra computes the "referential depth" of tables based on
// foreign-key constraints.
//
// The query is structured as follows:
//   - q: The schema to query.
//   - parent_refs: Identifies all index References as a mapping of
//     (owner, table) -> (owner, constraint).
//   - ref_to_tbl: Resolves the constraint name references to Primary or
//     Unique indexes to parent table names.
//   - tbl_to_parent: Joins parent_refs and ref_to_tbl to produce
//     child(owner, table) -> parent(owner, table) mappings.
//   - seeds: Emits a dummy row for all tables in the schema. This
//     ensures that tables with no children are still in the result set.
//   - top-level: A union of seeds and tbl_to_parent.
const depOrderTemplateOra = `
WITH
parent_refs AS (
    SELECT OWNER child_owner, TABLE_NAME child_name, R_OWNER parent_owner, R_CONSTRAINT_NAME ref_name
    FROM ALL_CONSTRAINTS
    WHERE OWNER = :owner AND CONSTRAINT_TYPE = 'R'),
ref_to_tbl AS (
    SELECT CONSTRAINT_NAME ref_name, OWNER parent_owner, TABLE_NAME parent_name
    FROM ALL_CONSTRAINTS
    WHERE OWNER = :owner AND CONSTRAINT_TYPE IN ('P', 'U')),
tbl_to_parent AS (
    SELECT NULL child_cat, child_owner, child_name, '' parent_cat, parent_owner, parent_name
    FROM ref_to_tbl
    JOIN parent_refs USING (parent_owner, ref_name)
    WHERE child_name != parent_name),
seeds AS (
    SELECT NULL child_cat, NULL child_owner, NULL child_name, '' parent_cat, OWNER parent_owner, TABLE_NAME parent_name
    FROM ALL_TABLES
    WHERE OWNER = :owner)
SELECT /*+  gather_plan_statistics */ * FROM seeds
UNION ALL
SELECT * from tbl_to_parent
`

// depOrderTemplatePg creates a mapping of child to parent tables.
//
// CTE elements:
//   - q: The catalog and schema to query
//   - constraints: Associates constraint ids with tables ids
//   - all_refs: Creates parent-child table name mappings. In
//     information_schema, an FK reference is expressed as a constraint
//     applied to some (child) table that references a unique constraint
//     on another (parent) table. This clause also filters out any
//     table self-references.
//   - refs: A recursive clause that chases parents to children,
//     potentially across schema boundaries. It starts by joining against
//     q and then fills in any child tables as needed.
//   - seeds: Emits a dummy row for all tables in the schema. This
//     ensures that tables with no children are still in the result set.
//   - top level: Union of refs and seeds.
//
// There's a hack for legacy versions of CRDB <= 23.1 which report an
// incorrect unique_constraint_schema. Cross-schema FK references should
// be relatively rare, and the FK constraint name can be made unique if
// required. https://github.com/cockroachdb/cockroach/issues/111419
const depOrderTemplatePg = `
WITH RECURSIVE
  q (table_catalog, table_schema) AS (VALUES ($1::TEXT, $2::TEXT)),
  constraints
    AS (
      SELECT table_catalog, table_schema, table_name,
             constraint_catalog, constraint_schema, constraint_name
      FROM %[1]s.information_schema.table_constraints
    ),
  all_refs
    AS (
      SELECT
        child.table_catalog AS child_catalog,
        child.table_schema AS child_schema,
        child.table_name AS child_table_name,
        parent.table_catalog AS parent_catalog,
        parent.table_schema AS parent_schema,
        parent.table_name AS parent_table_name
      FROM
        %[1]s.information_schema.referential_constraints AS ref
        JOIN constraints AS child ON
            ref.constraint_catalog = child.constraint_catalog
            AND ref.constraint_schema = child.constraint_schema
            AND ref.constraint_name = child.constraint_name
        JOIN constraints AS parent ON
            ref.unique_constraint_catalog = parent.table_catalog
            AND (CASE WHEN $3::BOOLEAN THEN
                   ref.unique_constraint_schema = child.table_schema
                ELSE
                   ref.unique_constraint_schema = parent.table_schema
                END)
            AND ref.unique_constraint_name = parent.constraint_name
      WHERE
        (child.table_catalog, child.table_schema, child.table_name)
        != (parent.table_catalog, parent.table_schema, parent.table_name)),
  refs AS (
    SELECT ar.* FROM all_refs ar
    JOIN q ON (ar.parent_catalog, ar.parent_schema) =
              (q.table_catalog, q.table_schema)
    UNION
    SELECT ar.* FROM all_refs ar
    JOIN refs r ON (ar.parent_catalog, ar.parent_schema, ar.parent_table_name) = 
                   (r.child_catalog, r.child_schema, r.child_table_name)
   ),
  tables
    AS (
      SELECT table_catalog, table_schema, table_name
      FROM %[1]s.information_schema.tables t
      WHERE table_type = 'BASE TABLE'
    ),
  seeds
    AS (
      SELECT
        NULL, NULL, NULL,
        table_catalog, table_schema, table_name
      FROM %[1]s.information_schema.tables t
      JOIN q USING (table_catalog, table_schema)
      WHERE table_type = 'BASE TABLE'
    )
SELECT * FROM refs UNION ALL SELECT * FROM seeds
`

// getDependencyRefs returns a map describing the parent-to-children
// relationships of tables. That is, the map values are the tables that
// have some immediate dependency on the key. Tables with no
// dependencies will have a zero-length slice as the value.
func getDependencyRefs(
	ctx context.Context, tx *types.TargetPool, db ident.Schema,
) (*ident.TableMap[[]ident.Table], error) {
	var args []any
	var stmt string
	switch tx.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		// Extract just the database name to refer to information_schema.
		parts := db.Idents(make([]ident.Ident, 0, 2))
		if len(parts) != 2 {
			return nil, errors.Errorf("expecting two schema parts, had %d", len(parts))
		}

		// See discussion on depOrderTemplatePg.
		legacyHack := false
		if tx.Product == types.ProductCockroachDB {
			modern, err := stdpool.CockroachMinVersion(tx.Version, "v23.2.0")
			if err != nil {
				return nil, err
			}
			legacyHack = !modern
		}

		stmt = fmt.Sprintf(depOrderTemplatePg, parts[0])
		args = []any{parts[0].Raw(), parts[1].Raw(), legacyHack}

	case types.ProductMariaDB, types.ProductMySQL:
		parts := db.Idents(make([]ident.Ident, 0, 1))
		if len(parts) != 1 {
			return nil, errors.Errorf("expecting one schema parts, had %d", len(parts))
		}
		stmt = depOrderTemplateMySQL
		// Catalog names are hardcoded to "def" in MySQL.
		args = []any{`def`, parts[0].Raw()}

	case types.ProductOracle:
		stmt = depOrderTemplateOra
		args = []any{sql.Named("owner", db.Raw())}
	default:
		return nil, errors.Errorf("getDependencyOrder unimplemented product: %s", tx.Product)
	}

	var ret *ident.TableMap[[]ident.Table]
	err := retry.Retry(ctx, tx, func(ctx context.Context) error {
		ret = &ident.TableMap[[]ident.Table]{}
		rows, err := tx.QueryContext(ctx, stmt, args...)
		if err != nil {
			return errors.Wrap(err, stmt)
		}
		defer func() { _ = rows.Close() }()

		// We have a switch statement here since PG-style databases have
		// an extra level in the namespace. Tables with no incoming
		// dependencies will have a single row with a NULL child table.
		for rows.Next() {
			var childDBRaw, childSchemaRaw, childTableRaw sql.NullString
			var parentDBRaw, parentSchemaRaw, parentTableRaw string

			if err := rows.Scan(&childDBRaw, &childSchemaRaw, &childTableRaw,
				&parentDBRaw, &parentSchemaRaw, &parentTableRaw); err != nil {
				return errors.WithStack(err)
			}

			var childSchema, parentSchema ident.Schema
			var childTableName, parentTableName ident.Ident
			switch tx.Product {
			case types.ProductCockroachDB, types.ProductPostgreSQL:
				parentSchema, err = ident.NewSchema(ident.New(parentDBRaw), ident.New(parentSchemaRaw))
				if err != nil {
					return err
				}
				parentTableName = ident.New(parentTableRaw)

				if childDBRaw.Valid && childSchemaRaw.Valid && childTableRaw.Valid {
					childSchema, err = ident.NewSchema(ident.New(childDBRaw.String), ident.New(childSchemaRaw.String))
					if err != nil {
						return err
					}
					childTableName = ident.New(childTableRaw.String)
				}

			default:
				parentSchema, err = ident.NewSchema(ident.New(parentSchemaRaw))
				if err != nil {
					return err
				}
				parentTableName = ident.New(parentTableRaw)

				if childSchemaRaw.Valid && childTableRaw.Valid {
					childSchema, err = ident.NewSchema(ident.New(childSchemaRaw.String))
					if err != nil {
						return err
					}
					childTableName = ident.New(childTableRaw.String)
				}
			}

			parentTable := ident.NewTable(parentSchema, parentTableName)
			if parentTable.Empty() {
				// Sanity-check, this should not happen.
				return errors.New("created an empty parent table")
			}

			// We want to ensure that root tables have an entry, even if
			// it's zero-length.
			children := ret.GetZero(parentTable)
			if !childSchema.Empty() && !childTableName.Empty() {
				childTable := ident.NewTable(childSchema, childTableName)
				children = append(children, childTable)
				log.Tracef("schema query: parent %s -> child %s", parentTable, childTable)
			}
			ret.Put(parentTable, children)
		}
		return errors.WithStack(rows.Err())
	})
	return ret, err
}
