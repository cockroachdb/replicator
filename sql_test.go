// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

func TestGetPrimaryKeyColumns(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	testcases := []struct {
		tableSchema string
		primaryKeys []string
	}{
		{
			"a INT",
			[]string{"rowid"},
		},
		{
			"a INT PRIMARY KEY",
			[]string{"a"},
		},
		{
			"a INT, b INT, PRIMARY KEY (a,b)",
			[]string{"a", "b"},
		},
		{
			"a INT, b INT, PRIMARY KEY (b,a)",
			[]string{"b", "a"},
		},
		{
			"a INT, b INT, c INT, PRIMARY KEY (b,a,c)",
			[]string{"b", "a", "c"},
		},
	}

	for i, test := range testcases {
		t.Run(fmt.Sprintf("%d:%s", i, test.tableSchema), func(t *testing.T) {
			a := assert.New(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			tableFullName := fmt.Sprintf("%s.test_%d", dbName, i)
			if !a.NoError(Execute(ctx, db,
				fmt.Sprintf(`CREATE TABLE %s ( %s )`, tableFullName, test.tableSchema))) {
				return
			}
			columns, err := GetPrimaryKeyColumns(ctx, db, tableFullName)
			if !a.NoError(err) {
				return
			}
			a.Equal(test.primaryKeys, columns)
		})
	}
}
