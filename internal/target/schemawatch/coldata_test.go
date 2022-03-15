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

// This file contains code repackaged from sql_test.go.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/stretchr/testify/assert"
)

func TestGetColumns(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	// Create the test db
	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	type testcase struct {
		tableSchema string
		primaryKeys []string
		dataCols    []string
	}
	testcases := []testcase{
		{
			"a INT",
			[]string{"rowid"},
			[]string{"a"},
		},
		{
			"a INT PRIMARY KEY",
			[]string{"a"},
			nil,
		},
		{
			"a INT, b INT",
			[]string{"rowid"},
			[]string{"a", "b"},
		},
		{
			"a INT, b INT, PRIMARY KEY (a,b)",
			[]string{"a", "b"},
			nil,
		},
		{
			"a INT, b INT, PRIMARY KEY (b,a)",
			[]string{"b", "a"},
			nil,
		},
		{
			"a INT, b INT, c INT, PRIMARY KEY (b,a,c)",
			[]string{"b", "a", "c"},
			nil,
		},
		{
			"a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c)",
			[]string{"b", "a", "c"},
			[]string{"q", "r"},
		},
		{
			"a INT, b INT, r INT, c INT, q INT, PRIMARY KEY (b,a,c) USING HASH WITH BUCKET_COUNT = 8",
			[]string{"ignored_crdb_internal_a_b_c_shard_8", "b", "a", "c"},
			[]string{"q", "r"},
		},
		// Ensure that computed columns are ignored.
		{
			tableSchema: "a INT, b INT, " +
				"c INT AS (a + b) STORED, " +
				"PRIMARY KEY (a,b)",
			primaryKeys: []string{"a", "b"},
			dataCols:    []string{"ignored_c"},
		},
		// Ensure that the PK constraint may have an arbitrary name.
		{
			"a INT, b INT, CONSTRAINT foobar_pk PRIMARY KEY (a,b)",
			[]string{"a", "b"},
			nil,
		},
		// Check non-interference from secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c), INDEX (c,a,b)",
			[]string{"b", "a", "c"},
			[]string{"q", "r"},
		},
		// Check non-interference from unique secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, PRIMARY KEY (b,a,c), UNIQUE INDEX (c,a,b)",
			[]string{"b", "a", "c"},
			[]string{"q", "r"},
		},
		// Check no-PK, but with a secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, INDEX (c,a,b)",
			[]string{"rowid"},
			[]string{"a", "b", "c", "q", "r"},
		},
		// Check no-PK, but with a unique secondary index.
		{
			"a INT, b INT, q INT, c INT, r INT, UNIQUE INDEX (c,a,b)",
			[]string{"rowid"},
			[]string{"a", "b", "c", "q", "r"},
		},
	}

	// Virtual columns not supported before v21.1.
	// Oldest target is v20.2.
	if !strings.Contains(dbInfo.Version(), "v20.2.") {
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

	for i, test := range testcases {
		t.Run(fmt.Sprintf("%d:%s", i, test.tableSchema), func(t *testing.T) {
			a := assert.New(t)

			tableName := ident.NewTable(dbName, ident.Public, ident.Newf("test_%d", i))
			if !a.NoError(retry.Execute(ctx, dbInfo.Pool(),
				fmt.Sprintf(`CREATE TABLE %s ( %s )`, tableName, test.tableSchema))) {
				return
			}
			colData, err := getColumns(ctx, dbInfo.Pool(), tableName)
			if !a.NoError(err) {
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
		})
	}
}
