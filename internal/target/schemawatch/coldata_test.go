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
		tableSchema string
		primaryKeys []string
		dataCols    []string
		check       func(*testing.T, []types.ColData)
	}
	testcases := []testcase{
		{
			"", // It's legal to create a table with no columns.
			[]string{"rowid"},
			nil,
			nil,
		},
		{
			"a INT",
			[]string{"rowid"},
			[]string{"a"},
			nil,
		},
		{
			"a INT PRIMARY KEY",
			[]string{"a"},
			nil,
			nil,
		},
		{
			"a INT, b INT",
			[]string{"rowid"},
			[]string{"a", "b"},
			nil,
		},
		{
			"a INT, b INT, PRIMARY KEY (a,b)",
			[]string{"a", "b"},
			nil,
			nil,
		},
		{
			"a INT, b INT, PRIMARY KEY (b,a)",
			[]string{"b", "a"},
			nil,
			nil,
		},
		{
			"a INT, b INT, c INT, PRIMARY KEY (b,a,c)",
			[]string{"b", "a", "c"},
			nil,
			nil,
		},
		{
			"a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c)",
			[]string{"b", "a", "c"},
			[]string{"q", "r"},
			nil,
		},
		{
			"a INT, b INT, r INT, c INT, q INT, PRIMARY KEY (b,a,c) USING HASH WITH BUCKET_COUNT = 8",
			[]string{"ignored_crdb_internal_a_b_c_shard_8", "b", "a", "c"},
			[]string{"q", "r"},
			nil,
		},
		// Ensure that computed data columns are ignored.
		{
			"a INT, b INT, " +
				"c INT AS (a + b) STORED, " +
				"PRIMARY KEY (a,b)",
			[]string{"a", "b"},
			[]string{"ignored_c"},
			nil,
		},
		// Ensure that computed pk columns are retained.
		{
			"a INT, b INT, " +
				"c INT AS (a + b) STORED, " +
				"PRIMARY KEY (a,c,b)",
			[]string{"a", "ignored_c", "b"},
			nil,
			nil,
		},
		// Ensure that the PK constraint may have an arbitrary name.
		{
			"a INT, b INT, CONSTRAINT foobar_pk PRIMARY KEY (a,b)",
			[]string{"a", "b"},
			nil,
			nil,
		},
		// Check non-interference from secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c), INDEX (c,a,b)",
			[]string{"b", "a", "c"},
			[]string{"q", "r"},
			nil,
		},
		// Check non-interference from unique secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c), UNIQUE INDEX (c,a,b)",
			[]string{"b", "a", "c"},
			[]string{"q", "r"},
			nil,
		},
		// Check no-PK, but with a secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, INDEX (c,a,b)",
			[]string{"rowid"},
			[]string{"a", "b", "c", "q", "r"},
			nil,
		},
		// Check no-PK, but with a unique secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, UNIQUE INDEX (c,a,b)",
			[]string{"rowid"},
			[]string{"a", "b", "c", "q", "r"},
			nil,
		},
		{
			fmt.Sprintf(`a %s."MyEnum" PRIMARY KEY`, fixture.TestDB.Ident()),
			[]string{"a"},
			nil,
			func(t *testing.T, data []types.ColData) {
				a := assert.New(t)
				if a.Len(data, 1) {
					col := data[0]
					a.Equal(
						ident.NewUDT(fixture.TestDB.Ident(), ident.Public, ident.New("MyEnum")),
						col.Type)
				}
			},
		},
	}

	// Virtual columns not supported before v21.1.
	// Oldest target is v20.2.
	if !strings.Contains(fixture.TargetPool.Version, "v20.2.") {
		testcases = append(testcases,
			testcase{
				tableSchema: "a INT, b INT, " +
					"c INT AS (a + b) STORED, " +
					"d INT AS (a + b) VIRTUAL, " +
					"PRIMARY KEY (a,b)",
				primaryKeys: []string{"a", "b"},
				dataCols:    []string{"ignored_c", "ignored_d"},
			},
		)
	}

	// Verify user-defined types with mixed-case name.
	if _, err := fixture.TargetPool.ExecContext(ctx, fmt.Sprintf(
		`CREATE TYPE %s."MyEnum" AS ENUM ('foo', 'bar')`,
		fixture.TestDB.Ident()),
	); !a.NoError(err) {
		return
	}

	for i, test := range testcases {
		t.Run(fmt.Sprintf("%d:%s", i, test.tableSchema), func(t *testing.T) {
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
			colData, ok := fixture.Watcher.Snapshot(tableName.AsSchema()).Columns[tableName]
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

	colData, ok := fixture.Watcher.Snapshot(tableName.AsSchema()).Columns[tableName]
	a.True(ok)
	a.NotNil(colData)

	viewData, ok := fixture.Watcher.Snapshot(tableName.AsSchema()).Columns[viewName]
	a.False(ok)
	a.Nil(viewData)
}
