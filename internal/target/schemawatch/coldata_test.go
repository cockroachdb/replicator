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

package schemawatch_test

// This file contains code repackaged from sql_test.go.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestGetColumns(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := all.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	type testcase struct {
		check       func(*testing.T, []types.ColData) // Optional extra test code
		dataCols    []string                          // Expected non-PK columns, in order
		products    []types.Product                   // Skip if not applicable to current product
		primaryKeys []string                          // Primary key columns, in order
		skip        bool                              // Extra logic to disable case
		sqlPost     []string                          // Additional SQL, table name is %s
		tableSchema string                            // Column definitions only
		types       *ident.Map[string]                // Optional check for column types
	}
	testcases := []testcase{
		{
			// It's legal to create a table with no columns.
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			primaryKeys: []string{"rowid"},
		},
		{
			tableSchema: "a INT",
			primaryKeys: []string{"rowid"},
			dataCols:    []string{"a"},
		},
		{
			tableSchema: "a INT PRIMARY KEY",
			primaryKeys: []string{"a"},
		},
		{
			tableSchema: "a INT, b INT",
			primaryKeys: []string{"rowid"},
			dataCols:    []string{"a", "b"},
		},
		// Basic array type.
		// Multidimensional array type. Not supported in CRDB yet.
		// https://github.com/cockroachdb/cockroach/issues/32552
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: "a INT8 PRIMARY KEY, b INT8[]",
			primaryKeys: []string{"a"},
			dataCols:    []string{"b"},
			types: ident.MapOf[string](
				"a", "INT8",
				"b", "INT8[]",
			),
		},
		{
			tableSchema: "a INT, b INT, PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
		},
		{
			tableSchema: "a INT, b INT, PRIMARY KEY (b,a)",
			primaryKeys: []string{"b", "a"},
		},
		{
			tableSchema: "a INT, b INT, c INT, PRIMARY KEY (b,a,c)",
			primaryKeys: []string{"b", "a", "c"},
		},
		{
			tableSchema: "a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c)",
			primaryKeys: []string{"b", "a", "c"},
			dataCols:    []string{"q", "r"},
		},
		{
			products:    []types.Product{types.ProductCockroachDB},
			tableSchema: "a INT, b INT, r INT, c INT, q INT, PRIMARY KEY (b,a,c) USING HASH WITH BUCKET_COUNT = 8",
			primaryKeys: []string{"ignored_crdb_internal_a_b_c_shard_8", "b", "a", "c"},
			dataCols:    []string{"q", "r"},
		},
		// Ensure that computed data columns are ignored.
		{
			// Virtual not implemented by any version of PostgreSQL.
			products: []types.Product{types.ProductCockroachDB, types.ProductOracle},
			tableSchema: "a INT, b INT, " +
				"c INT GENERATED ALWAYS AS (a + b) VIRTUAL, " +
				"PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
			dataCols:    []string{"ignored_c"},
		},
		{
			products: []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: "a INT, b INT, " +
				"c INT GENERATED ALWAYS AS (a + b) STORED, " +
				"PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
			dataCols:    []string{"ignored_c"},
			// Generated columns first added in PG12
			skip: strings.Contains(fixture.TargetPool.Version, "PostgreSQL 11"),
		},
		// Ensure that computed pk columns are retained.
		{
			// Virtual not implemented by any version of PostgreSQL.
			products: []types.Product{types.ProductCockroachDB},
			tableSchema: "a INT, b INT, " +
				"c INT GENERATED ALWAYS AS (a + b) VIRTUAL, " +
				"PRIMARY KEY (a,c,b)",
			primaryKeys: []string{"a", "ignored_c", "b"},
			// Virtual PK columns not supported before 22.X releases.
			skip: strings.Contains(fixture.TargetPool.Version, "v21."),
		},
		{
			// Virtual not implemented by any version of PostgreSQL.
			products: []types.Product{types.ProductCockroachDB},
			tableSchema: "a INT, b INT, " +
				"c INT GENERATED ALWAYS AS (a + b) STORED, " +
				"d INT GENERATED ALWAYS AS (a + b) VIRTUAL, " +
				"PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
			dataCols:    []string{"ignored_c", "ignored_d"},
			skip: fixture.TargetPool.Product == types.ProductPostgreSQL &&
				strings.Contains(fixture.TargetPool.Version, "PostgreSQL 11"),
		},
		// Ensure that the PK constraint may have an arbitrary name.
		{
			tableSchema: "a INT, b INT, CONSTRAINT foobar_pk PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
		},
		// Check non-interference from secondary index.
		{
			tableSchema: "a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c)",
			primaryKeys: []string{"b", "a", "c"},
			dataCols:    []string{"q", "r"},
			sqlPost:     []string{"CREATE INDEX ind_cab1 ON %s (c,a,b)"},
		},
		// Check non-interference from unique secondary index.
		{
			tableSchema: "a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c)",
			primaryKeys: []string{"b", "a", "c"},
			dataCols:    []string{"q", "r"},
			sqlPost:     []string{"CREATE UNIQUE INDEX ind_cab2 ON %s (c,a,b)"},
		},
		// Check no-PK, but with a secondary index.
		{
			tableSchema: "a INT, b INT, q INT, c INT, r INT",
			primaryKeys: []string{"rowid"},
			dataCols:    []string{"a", "b", "c", "q", "r"},
			sqlPost:     []string{"CREATE INDEX ind_cab3 ON %s (c,a,b)"},
		},
		// Check no-PK, but with a unique secondary index.
		{
			tableSchema: "a INT, b INT, q INT, c INT, r INT",
			primaryKeys: []string{"rowid"},
			dataCols:    []string{"a", "b", "c", "q", "r"},
			sqlPost:     []string{"CREATE UNIQUE INDEX ind_cab4 ON %s (c,a,b)"},
		},
		// UDT enum test with boring case.
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: fmt.Sprintf(`a %s.boring_enum PRIMARY KEY`, fixture.TargetSchema.Schema()),
			primaryKeys: []string{"a"},
			types: ident.MapOf[string](
				"a", fixture.TargetSchema.Schema().String()+`."boring_enum"`,
			),
		},
		// UDT enum test with mixed case.
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: fmt.Sprintf(`a %s."MyEnum" PRIMARY KEY`, fixture.TargetSchema.Schema()),
			primaryKeys: []string{"a"},
			types: ident.MapOf[string](
				"a", fixture.TargetSchema.Schema().String()+`."MyEnum"`,
			),
		},
		// Check array of boring-case UDT enum.
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: fmt.Sprintf(`pk INT8 PRIMARY KEY, val %s.boring_enum[]`, fixture.TargetSchema.Schema()),
			primaryKeys: []string{"pk"},
			dataCols:    []string{"val"},
			types: ident.MapOf[string](
				"pk", "INT8",
				"val", fixture.TargetSchema.Schema().String()+`."boring_enum"[]`,
			),
		},
		// Check array of mixed-case UDT enum.
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: fmt.Sprintf(`pk INT8 PRIMARY KEY, val %s."MyEnum"[]`, fixture.TargetSchema.Schema()),
			primaryKeys: []string{"pk"},
			dataCols:    []string{"val"},
			types: ident.MapOf[string](
				"pk", "INT8",
				"val", fixture.TargetSchema.Schema().String()+`."MyEnum"[]`,
			),
		},
		// Check enums for mySQL.
		{
			products: []types.Product{types.ProductMySQL},
			tableSchema: `pk INT PRIMARY KEY, a ENUM('x-small', 'small', 'medium', 'large', 'x-large'), 
			              b SET('x-small', 'small', 'medium', 'large', 'x-large')`,
			primaryKeys: []string{"pk"},
			dataCols:    []string{"a", "b"},
			types: ident.MapOf[string](
				"pk", "int",
				"a", "enum",
				"b", "set",
			),
		},
		// Check other mySQL types.
		{
			products: []types.Product{types.ProductMySQL},
			tableSchema: `
			pk INT PRIMARY KEY, 
			a BIGINT,
			b TIMESTAMP,
			c DATETIME,
			d NUMERIC,
			e DECIMAL,
			f DECIMAL(10,2),
			g VARCHAR(10),
			h FLOAT(8),
		    i CHAR(1)
			`,
			primaryKeys: []string{"pk"},
			dataCols:    []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"},
			types: ident.MapOf[string](
				"pk", "int",
				"a", "bigint",
				"b", "timestamp",
				"c", "datetime",
				"d", "decimal(10,0)",
				"e", "decimal(10,0)",
				"f", "decimal(10,2)",
				"g", "varchar(10)",
				"h", "float",
				"i", "char(1)",
			),
		},
		// Check type extraction.
		{
			products:    []types.Product{types.ProductOracle},
			tableSchema: "a INT, b VARCHAR(42), c FLOAT(8), d RAW(55), e NUMBER(4,2), PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
			dataCols:    []string{"c", "d", "e"},
			types: ident.MapOf[string](
				"a", "NUMBER",
				"b", "VARCHAR2(42)",
				"c", "FLOAT(8)",
				"d", "RAW(55)",
				"e", "NUMBER(4,2)",
			),
		},
		// Check default value extraction
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductMySQL, types.ProductPostgreSQL, types.ProductOracle},
			tableSchema: "a INT PRIMARY KEY, b VARCHAR(2048) DEFAULT 'Hello World!'",
			primaryKeys: []string{"a"},
			dataCols:    []string{"b"},
			check: func(t *testing.T, data []types.ColData) {
				for _, col := range data {
					if ident.Equal(col.Name, ident.New("b")) {
						// Different versions of CRDB will return
						// varying type-assertion syntax.
						assert.Contains(t, col.DefaultExpr, "'Hello World!'")
						return
					}
				}
				assert.Fail(t, "did not find b column")
			},
		},
		// Check default value extraction with embedded single quotes in MySQL
		{
			products:    []types.Product{types.ProductMySQL},
			tableSchema: "a INT PRIMARY KEY, b VARCHAR(2048) DEFAULT 'Hello''World!'",
			primaryKeys: []string{"a"},
			dataCols:    []string{"b"},
			check: func(t *testing.T, data []types.ColData) {
				for _, col := range data {
					if ident.Equal(col.Name, ident.New("b")) {
						assert.Contains(t, col.DefaultExpr, "'Hello''World!'")
						return
					}
				}
				assert.Fail(t, "did not find b column")
			},
		},
		// Default with a function.
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: "a INT PRIMARY KEY, b VARCHAR(2048) DEFAULT replace('Hello World!', ' ', '+')",
			primaryKeys: []string{"a"},
			dataCols:    []string{"b"},
			check: func(t *testing.T, data []types.ColData) {
				a := assert.New(t)
				for _, col := range data {
					if ident.Equal(col.Name, ident.New("b")) {
						// Type assertion syntax will vary between CRDB versions.
						a.True(
							strings.HasPrefix(col.DefaultExpr, "replace('Hello World!'"),
							col.DefaultExpr)
						a.True(strings.HasSuffix(col.DefaultExpr, ")"), col.DefaultExpr)
						return
					}
				}
				a.Fail("did not find b column")
			},
		},
	}

	switch fixture.TargetPool.Product {
	case types.ProductMySQL:
		// MySQL does not support TYPES
	default:
		// Enum with a boring name.
		if _, err := fixture.TargetPool.ExecContext(ctx, fmt.Sprintf(
			`CREATE TYPE %s.boring_enum AS ENUM ('foo', 'bar')`,
			fixture.TargetSchema.Schema()),
		); !a.NoError(err) {
			return
		}

		// Verify user-defined types with mixed-case name.
		if _, err := fixture.TargetPool.ExecContext(ctx, fmt.Sprintf(
			`CREATE TYPE %s."MyEnum" AS ENUM ('foo', 'bar')`,
			fixture.TargetSchema.Schema()),
		); !a.NoError(err) {
			return
		}
	}

	for i, test := range testcases {
		t.Run(fmt.Sprintf("%d:%s", i, test.tableSchema), func(t *testing.T) {
			if test.skip {
				t.Skip("not applicable")
			}
			if len(test.products) > 0 {
				productMatches := false
				for _, product := range test.products {
					if product == fixture.TargetPool.Product {
						productMatches = true
						break
					}
				}
				if !productMatches {
					t.Skipf("testcase not relevant for current product")
				}
			}
			a := assert.New(t)

			cmd := fmt.Sprintf(`CREATE TABLE %%s ( %s )`, test.tableSchema)

			// Hack to set session variable for hash-sharded indexes.
			if strings.Contains(cmd, "USING HASH") &&
				(strings.Contains(fixture.TargetPool.Version, "v20.") ||
					strings.Contains(fixture.TargetPool.Version, "v21.")) {
				cmd = "SET experimental_enable_hash_sharded_indexes='true';" + cmd
			}

			ti, err := fixture.CreateTargetTable(ctx, cmd)
			if !a.NoError(err) {
				return
			}
			tableName := ti.Name()

			for _, cmd := range test.sqlPost {
				_, err := fixture.TargetPool.ExecContext(ctx, fmt.Sprintf(cmd, tableName))
				if !a.NoError(err, cmd) {
					return
				}
			}

			colData, ok := fixture.Watcher.Get().Columns.Get(tableName)
			if !a.Truef(ok, "Snapshot() did not return info for %s", tableName) {
				return
			}
			var primaryKeys, dataCols []string
			for i := range colData {
				a.NotEmpty(colData[i].Type)
				name := colData[i].Name.Canonical().Raw()
				if colData[i].Ignored {
					name = "ignored_" + name
				}
				if colData[i].Primary {
					a.Empty(dataCols, "should see PKs before data colums")
					primaryKeys = append(primaryKeys, name)
				} else {
					dataCols = append(dataCols, name)
				}
				if test.types != nil {
					// See above comment.
					a.Equalf(test.types.GetZero(colData[i].Name),
						colData[i].Type,
						"column %s", colData[i].Name)
				}

			}
			a.Equal(test.primaryKeys, primaryKeys, "primary keys")
			a.Equal(test.dataCols, dataCols, "data columns")
			if test.check != nil {
				test.check(t, colData)
			}
		})
	}
}

// Ensure that only tables are loaded.
func TestColDataIgnoresViews(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := all.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	ti, err := fixture.CreateTargetTable(ctx, `CREATE TABLE %s ( pk INT PRIMARY KEY )`)
	if !a.NoError(err) {
		return
	}
	tableName := ti.Name()

	vi, err := fixture.CreateTargetTable(ctx, fmt.Sprintf(
		`CREATE VIEW %%s AS SELECT pk FROM %s`, tableName))
	if !a.NoError(err) {
		return
	}
	viewName := vi.Name()

	colData, ok := fixture.Watcher.Get().Columns.Get(tableName)
	a.True(ok)
	a.NotNil(colData)

	viewData, ok := fixture.Watcher.Get().Columns.Get(viewName)
	a.False(ok)
	a.Nil(viewData)
}
