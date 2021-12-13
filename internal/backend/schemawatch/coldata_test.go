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
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/backend/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/stretchr/testify/assert"
)

func TestGetColumns(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	// Create the test db
	dbName, cancel, err := sinktest.CreateDb(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	testcases := []struct {
		tableSchema string
		primaryKeys []string
		dataCols    []string
	}{
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
			[]string{"b", "a", "c"},
			[]string{"q", "r"},
		},
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
				if colData[i].Primary {
					primaryKeys = append(primaryKeys, colData[i].Name.Raw())
				} else {
					dataCols = append(dataCols, colData[i].Name.Raw())
				}
			}
			a.Equal(test.primaryKeys, primaryKeys)
			a.Equal(test.dataCols, dataCols)
		})
	}
}
