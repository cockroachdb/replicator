package main

import (
	"fmt"
	"reflect"
	"testing"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

func TestGetPrimaryKeyColumns(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
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
			tableFullName := fmt.Sprintf("%s.test_%d", dbName, i)
			if err := Execute(
				db,
				fmt.Sprintf(`CREATE TABLE %s ( %s )`, tableFullName, test.tableSchema),
			); err != nil {
				t.Fatal(err)
			}
			columns, err := GetPrimaryKeyColumns(db, tableFullName)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.primaryKeys, columns) {
				t.Errorf("expected %v, got %v", test.primaryKeys, columns)
			}
		})
	}
}
