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

var pgCatalog = ident.New("pg_catalog")

func colSliceEqual(a, b []types.ColData) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

// Retrieve the primary key columns in their index-order.
// Note: Differs from MySQL in how we detect JSON columns.
//
// Parts of the CTE:
// * pk_constraints: finds the primary key constraint for the table
// https://dev.mysql.com/doc/refman/8.0/en/information-schema-table-constraints-table.html
// * json: in MariaDB json type is a longtext with a CHECK(json_valid) constraint
// * pks: extracts the names of the PK columns and their relative
// positions.
// * cols: extracts all columns, ignoring those with generation
// expressions by checking the "extra" column.
// The type of the column is derived from the data_type or the column_type.
// The column type may have additional information (e.g. precision),
// which may be required when casting types while applying mutations to the
// target database.
// https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html
// * ordered: adds a synthetic seq_in_index to the non-PK columns.
// * SELECT: aggregates the above, sorting the PK columns in-order
// before the non-PK columns.
const sqlColumnsQueryMariaDB = `
WITH pk_constraints AS (
	SELECT
		   table_schema,
		   table_name,
		   constraint_name
	  FROM information_schema.table_constraints
	 WHERE constraint_type = 'PRIMARY KEY'
),
json as (
	SELECT
		   constraint_schema as  table_schema,
		   table_name,
		   constraint_name as column_name
	  FROM information_schema.check_constraints
	 WHERE check_clause LIKE 'json_valid(%'
),
pks AS (
	SELECT
		   table_schema,
		   table_name,
		   column_name,
		   ordinal_position
	  FROM information_schema.key_column_usage
      JOIN pk_constraints USING (table_schema, table_name, constraint_name)
),
cols AS (
	SELECT
		   table_schema,
		   table_name,
		   column_name,
		   CASE
			 WHEN data_type in ('decimal','char','varchar') THEN column_type
			 ELSE data_type
		   END as data_type,
		   column_type,
		   column_default,
		   extra IN ('STORED GENERATED', 'VIRTUAL GENERATED') AS ignored
	FROM information_schema.columns
),
ordered AS (
	SELECT
		   table_schema,
		   table_name,
		   column_name,
		   MIN(
		     COALESCE(pks.ordinal_position, 2048)
		   ) AS ordinal_position
	  FROM cols
		   LEFT JOIN pks USING (table_schema, table_name, column_name)
  GROUP BY table_schema, table_name, column_name
)
   SELECT
		  column_name,
		  pks.ordinal_position IS NOT NULL,
		  CASE
		    WHEN json.column_name = cols.column_name THEN 'json'
		    ELSE data_type
		  END as data_type,
		  column_default,
		  ignored
      FROM cols
	       JOIN ordered USING (table_schema, table_name, column_name)
		   LEFT JOIN pks USING (table_schema, table_name, column_name)
		   LEFT JOIN json USING (table_schema, table_name, column_name)
   WHERE table_schema = ?
     AND table_name = ?
ORDER BY ordered.ordinal_position, column_name
`

// Retrieve the primary key columns in their index-order.
//
// Parts of the CTE:
// * pk_constraints: finds the primary key constraint for the table
// https://dev.mysql.com/doc/refman/8.0/en/information-schema-table-constraints-table.html
// * pks: extracts the names of the PK columns and their relative
// positions.
// * cols: extracts all columns, ignoring those with generation
// expressions by checking the "extra" column.
// The default expression is quoted if it is a string,
// unless it's an expression (extras=DEFAULT_GENERATED).
// The type of the column is derived from the data_type or the column_type.
// The column type may have additional information (e.g. precision),
// which may be required when casting types while applying mutations to the
// target database.
// https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html
// * ordered: adds a synthetic seq_in_index to the non-PK columns.
// * SELECT: aggregates the above, sorting the PK columns in-order
// before the non-PK columns.
const sqlColumnsQueryMySQL = `
WITH pk_constraints AS (
	SELECT
		   table_schema,
		   table_name,
		   constraint_name
	  FROM information_schema.table_constraints
	 WHERE constraint_type = 'PRIMARY KEY'
   ),
pks AS (
SELECT
	   table_schema,
	   table_name,
	   column_name,
	   ordinal_position
  FROM information_schema.key_column_usage
  JOIN pk_constraints USING (table_schema, table_name, constraint_name)
),
cols AS (
  SELECT
		 table_schema,
		 table_name,
		 column_name,
		 CASE
			WHEN data_type in ('decimal','char','varchar') THEN column_type
			ELSE data_type
		 END as data_type,
		 column_type,
		 CASE
			WHEN extra = 'DEFAULT_GENERATED'
		         OR data_type NOT in ('char','varchar', 'text')
			     OR column_default IS NULL 
			THEN column_default
			ELSE quote (column_default)
	     END as column_default,
		 extra IN ('STORED GENERATED', 'VIRTUAL GENERATED') AS ignored
	FROM information_schema.columns
),
ordered AS (
	  SELECT
			 table_schema,
			 table_name,
			 column_name,
			 min(
			  COALESCE(pks.ordinal_position, 2048)
			 ) AS ordinal_position
		FROM cols
			 LEFT JOIN pks USING (table_schema, table_name, column_name)
	GROUP BY table_schema, table_name, column_name
   )
SELECT column_name, pks.ordinal_position IS NOT NULL, data_type, column_default, ignored
FROM cols
JOIN ordered USING (table_schema, table_name, column_name)
LEFT JOIN pks USING (table_schema, table_name, column_name)
WHERE table_schema = ?
AND table_name = ?
ORDER BY ordered.ordinal_position, column_name
`

// Retrieve the primary key columns in their index-order.
//
// Parts of the CTE:
// * atc: basic information about all columns
// * pk_cols:  primary-key constraints, which provide PK column ordering
// * acc: look for the ids of constraints applied to the columns
//
// We extract the length for data types where a size is a mandatory
// feature of the SQL grammar.
//
// List of data types from:
// https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html
const sqlColumnsQueryOra = `
WITH atc AS (
  SELECT OWNER, TABLE_NAME, COLUMN_NAME,
  CASE
    WHEN CHAR_COL_DECL_LENGTH IS NOT NULL AND DATA_TYPE NOT LIKE '%LOB%' THEN
      DATA_TYPE || '(' || CHAR_COL_DECL_LENGTH || ')'
    WHEN DATA_PRECISION IS NOT NULL AND DATA_SCALE IS NOT NULL THEN
      DATA_TYPE || '(' || DATA_PRECISION || ',' || DATA_SCALE || ')'
    WHEN DATA_PRECISION IS NOT NULL THEN
      DATA_TYPE || '(' || DATA_PRECISION || ')'
    WHEN DATA_TYPE IN ('RAW', 'UROWID') THEN
      DATA_TYPE || '(' || DATA_LENGTH || ')'
    ELSE DATA_TYPE
  END DATA_TYPE,
  DATA_DEFAULT,
  VIRTUAL_COLUMN FROM ALL_TAB_COLS
),
     pk_cols  AS (SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE CONSTRAINT_TYPE='P'),
     acc AS (SELECT OWNER, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, POSITION, 't' IS_PK FROM ALL_CONS_COLUMNS)
SELECT COLUMN_NAME,
       COALESCE(IS_PK, 'f'),
       atc.DATA_TYPE,
       atc.DATA_DEFAULT,
       CASE WHEN atc.VIRTUAL_COLUMN = 'YES' THEN 't' ELSE 'f' END
FROM atc
LEFT JOIN pk_cols  USING (OWNER, TABLE_NAME)
LEFT JOIN acc USING (OWNER, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME)
WHERE (OWNER = :owner AND TABLE_NAME = :tbl_name)
ORDER BY POSITION, COLUMN_NAME
`

// Retrieve the primary key columns in their index-order, then append
// any remaining non-generated columns.
//
// Parts of the CTE:
// * pk_constraints: finds the primary key constraint for the table
// * pks: extracts the names of the PK columns and their relative
// positions. We exclude any "storing" columns to account for rowid
// value.
// * cols: extracts all columns, ignoring those with generation
// expressions (e.g. hash-sharded index clustering column). The NOT IN
// clause supports CRDB <= v22.1 that return a non-standard 'NO' value.
// * ordered: adds a synthetic seq_in_index to the non-PK columns.
// * SELECT: aggregates the above, sorting the PK columns in-order
// before the non-PK columns.
const sqlColumnsQueryPg = `
    WITH pk_constraints AS (
                  SELECT table_catalog,
                         table_schema,
                         table_name,
                         constraint_name
                    FROM %[1]s.information_schema.table_constraints
                   WHERE constraint_type = 'PRIMARY KEY'
                 ),
         pks AS (
              SELECT table_catalog,
                     table_schema,
                     table_name,
                     column_name,
                     ordinal_position
                FROM %[1]s.information_schema.key_column_usage
                JOIN pk_constraints USING (table_catalog, table_schema, table_name, constraint_name)
             ),
         cols AS (
                SELECT table_catalog,
                       table_schema,
                       table_name,
                       column_name,
                       quote_ident(udt_catalog) || '.' || quote_ident(udt_schema) || '.' || quote_ident(udt_name) ||
                       CASE WHEN collation_name IS NOT NULL THEN ' COLLATE ' || collation_name ELSE '' END AS data_type,
                       column_default,
                       is_generated NOT IN ('NEVER', 'NO') AS ignored
                  FROM %[1]s.information_schema.columns
              ),
         ordered AS (
                    SELECT table_catalog,
                           table_schema,
                           table_name,
                           column_name,
                           min(
                            COALESCE(pks.ordinal_position, 2048)
                           ) AS ordinal_position
                      FROM cols
                           LEFT JOIN pks USING (table_catalog, table_schema, table_name, column_name)
                  GROUP BY table_catalog, table_schema, table_name, column_name
                 )
  SELECT column_name, pks.ordinal_position IS NOT NULL, data_type, column_default, ignored
    FROM cols
    JOIN ordered USING (table_catalog, table_schema, table_name, column_name)
    LEFT JOIN pks USING (table_catalog, table_schema, table_name, column_name)
   WHERE table_catalog = $1
     AND table_schema = $2
     AND table_name = $3
ORDER BY ordered.ordinal_position, column_name`

// getColumns returns the column names for the primary key columns in
// their index-order, followed by all other columns that should be
// mutated.
func getColumns(
	ctx context.Context, tx *types.TargetPool, table ident.Table,
) ([]types.ColData, error) {
	var args []any
	var stmt string
	switch tx.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		parts := table.Idents(make([]ident.Ident, 0, 3))
		if len(parts) != 3 {
			return nil, errors.Errorf("expecting three table name parts, had %d", len(parts))
		}
		stmt = fmt.Sprintf(sqlColumnsQueryPg, parts[0])
		args = []any{
			parts[0].Raw(),
			parts[1].Raw(),
			parts[2].Raw(),
		}
	case types.ProductMariaDB:
		parts := table.Idents(make([]ident.Ident, 0, 2))
		if len(parts) != 2 {
			return nil, errors.Errorf("expecting two table name parts, had %d", len(parts))
		}
		stmt = sqlColumnsQueryMariaDB
		args = []any{
			parts[0].Raw(),
			parts[1].Raw(),
		}
	case types.ProductMySQL:
		parts := table.Idents(make([]ident.Ident, 0, 2))
		if len(parts) != 2 {
			return nil, errors.Errorf("expecting two table name parts, had %d", len(parts))
		}
		stmt = sqlColumnsQueryMySQL
		args = []any{
			parts[0].Raw(),
			parts[1].Raw(),
		}
	case types.ProductOracle:
		parts := table.Idents(make([]ident.Ident, 0, 2))
		if len(parts) != 2 {
			return nil, errors.Errorf("expecting two table name parts, had %d", len(parts))
		}
		stmt = sqlColumnsQueryOra
		args = []any{
			sql.Named("owner", parts[0].Raw()),
			sql.Named("tbl_name", parts[1].Raw()),
		}
	default:
		return nil, errors.Errorf("unimplemented: %s", tx.Product)
	}

	var columns []types.ColData
	err := retry.Retry(ctx, tx, func(ctx context.Context) error {
		rows, err := tx.QueryContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		// Clear from previous loop.
		columns = columns[:0]
		foundPrimay := false
		var appendLater []types.ColData
		for rows.Next() {
			var column types.ColData
			var defaultExpr sql.NullString
			var name string
			if err := rows.Scan(&name, &column.Primary, &column.Type, &defaultExpr, &column.Ignored); err != nil {
				return err
			}
			column.Name = ident.New(name)

			// Hack to force hash-sharded index columns out of PKs.
			primaryHack := strings.HasPrefix(name, "crdb_internal_")
			if primaryHack {
				column.Ignored = true
				column.Primary = false
			}
			if column.Primary {
				foundPrimay = true
			}
			switch tx.Product {
			case types.ProductCockroachDB, types.ProductPostgreSQL:
				if defaultExpr.Valid {
					column.DefaultExpr = defaultExpr.String
				}
				// Re-parse the type name so that we have a consistent
				// representation. For example, in the postgres query,
				// we use the quote_ident() function, which only adds
				// quotes if necessary. We'll also look for types
				// defined within the pg_catalog schema (i.e. built-in
				// types) and use simple names for them.
				parsed, err := ident.ParseTable(column.Type)
				if err != nil {
					return err
				}

				parts := parsed.Idents(make([]ident.Ident, 0, 3))
				if len(parts) != 3 {
					return errors.Errorf("expected 3, got %d parts when splitting ident: %s",
						len(parts), parsed)
				}

				lastRaw := parts[2].Raw()
				isArray := lastRaw[0] == '_'
				if isArray {
					// Drop the leading _ so we can append [] later on
					// to provide a consistent name.
					parts[2] = ident.New(lastRaw[1:])
				}

				if ident.Equal(parts[1], pgCatalog) {
					// This seems to be necessary for the pgx driver to not treat the "geometry" type as unknown.
					lastRaw = strings.ToUpper(lastRaw)
					if isArray {
						lastRaw = lastRaw[1:] + "[]"
					}
					column.Type = lastRaw
					break
				}

				column.Type = ident.NewTable(parsed.Schema(), parts[2]).String()
				if isArray {
					column.Type += "[]"
				}
			case types.ProductMariaDB:
				// In MariaDB we get either a null result or the "NULL" string
				// if there are no default expressions.
				if defaultExpr.String != "NULL" && defaultExpr.Valid {
					column.DefaultExpr = defaultExpr.String
				}
			case types.ProductMySQL:
				if defaultExpr.Valid {
					column.DefaultExpr = defaultExpr.String
				}
			case types.ProductOracle:
				if defaultExpr.Valid {
					// Oracle also likes to include some dangling whitespace.
					column.DefaultExpr = strings.TrimSpace(defaultExpr.String)
				}
			default:
				return errors.Errorf("unimplemented: %s", tx.Product)
			}

			column.Parse = parseHelper(tx.Product, column.Type)
			if primaryHack {
				appendLater = append(appendLater, column)
			} else {
				columns = append(columns, column)
			}
		}

		columns = append(columns, appendLater...)

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

			next := make([]types.ColData, 1, len(columns)+1)
			next[0] = types.ColData{
				Ignored: false,
				Name:    rowID,
				Primary: true,
				Type:    "INT8",
			}

			for _, col := range columns {
				if !ident.Equal(col.Name, rowID) {
					next = append(next, col)
				}
			}
			columns = next
		}

		return nil
	})
	return columns, err
}
