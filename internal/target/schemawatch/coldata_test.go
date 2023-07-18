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
		tableSchema string                            // Column definitions only
		sqlPost     []string                          // Additional SQL, table name is %s
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
			tableSchema: "a INT, b INT, " +
				"c INT AS (a + b) VIRTUAL, " +
				"PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
			dataCols:    []string{"ignored_c"},
		},
		// Ensure that computed pk columns are retained.
		{
			tableSchema: "a INT, b INT, " +
				"c INT AS (a + b) VIRTUAL, " +
				"PRIMARY KEY (a,c,b)",
			primaryKeys: []string{"a", "ignored_c", "b"},
		},
		{
			products: []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: "a INT, b INT, " +
				"c INT AS (a + b) STORED, " +
				"d INT AS (a + b) VIRTUAL, " +
				"PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
			dataCols:    []string{"ignored_c", "ignored_d"},
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
		{
			products:    []types.Product{types.ProductCockroachDB, types.ProductPostgreSQL},
			tableSchema: fmt.Sprintf(`a %s."MyEnum" PRIMARY KEY`, fixture.TestDB.Schema()),
			primaryKeys: []string{"a"},
			check: func(t *testing.T, data []types.ColData) {
				a := assert.New(t)
				if a.Len(data, 1) {
					col := data[0]
					a.Equal(
						ident.NewUDT(fixture.TestDB.Schema(), ident.New("MyEnum")),
						col.Type)
				}
			},
		},
	}

	// Verify user-defined types with mixed-case name.
	if _, err := fixture.TargetPool.ExecContext(ctx, fmt.Sprintf(
		`CREATE TYPE %s."MyEnum" AS ENUM ('foo', 'bar')`,
		fixture.TestDB.Schema()),
	); !a.NoError(err) {
		return
	}

	for i, test := range testcases {
		t.Run(fmt.Sprintf("%d:%s", i, test.tableSchema), func(t *testing.T) {
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

			ti, err := fixture.CreateTable(ctx, cmd)
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

			colData, ok := fixture.Watcher.Get().Columns[tableName]
			if !a.Truef(ok, "Snapshot() did not return info for %s", tableName) {
				return
			}
			var primaryKeys, dataCols []string
			for i := range colData {
				a.NotEmpty(colData[i].Type)
				name := colData[i].Name.Raw()
				if colData[i].Ignored {
					name = "ignored_" + name
				}
				if colData[i].Primary {
					primaryKeys = append(primaryKeys, name)
				} else {
					dataCols = append(dataCols, name)
				}
			}
			a.Equal(test.primaryKeys, primaryKeys)
			a.Equal(test.dataCols, dataCols)
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

	ti, err := fixture.CreateTable(ctx, `CREATE TABLE %s ( pk INT PRIMARY KEY )`)
	if !a.NoError(err) {
		return
	}
	tableName := ti.Name()

	vi, err := fixture.CreateTable(ctx, fmt.Sprintf(`CREATE VIEW %%s AS SELECT pk FROM %s`, tableName))
	if !a.NoError(err) {
		return
	}
	viewName := vi.Name()

	colData, ok := fixture.Watcher.Get().Columns[tableName]
	a.True(ok)
	a.NotNil(colData)

	viewData, ok := fixture.Watcher.Get().Columns[viewName]
	a.False(ok)
	a.Nil(viewData)
}
