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
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	log.SetLevel(log.TraceLevel)
	os.Exit(m.Run())
}

// TestGetDependencyRefs validates the query used to extract table
// relationship information.
//
// See also [types.SchemaData] for how this data is consumed.
func TestGetDependencyRefs(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	tcs := []struct {
		name             string
		schema           string
		expectedChildren []string
	}{
		{
			"parent",
			"create table %[1]s.parent (pk int primary key)",
			[]string{"child", "child_2", "three", "four", "five", "six"},
		},
		{
			"another_table",
			"create table %[1]s.another_table (pk int primary key)",
			[]string{},
		},
		{
			"unreferenced",
			"create table %[1]s.unreferenced (pk int primary key)",
			[]string{},
		},
		{
			"child",
			"create table %[1]s.child (pk int primary key, parent int, foreign key(parent) references %[1]s.parent(pk))",
			[]string{"grandchild", "grandchild_multi"},
		},
		{
			"child_2",
			"create table %[1]s.child_2 (pk int primary key, parent_2 int, foreign key(parent_2) references %[1]s.parent(pk))",
			[]string{"grandchild_2", "grandchild_multi"},
		},
		{
			"grandchild",
			"create table %[1]s.grandchild (pk int primary key, child int, foreign key(child) references %[1]s.child(pk))",
			[]string{"three"},
		},
		{
			"grandchild_2",
			"create table %[1]s.grandchild_2 (pk int primary key, child_2 int, foreign key(child_2) references %[1]s.child_2(pk))",
			[]string{},
		},
		{
			"grandchild_multi",
			"create table %[1]s.grandchild_multi (pk int primary key, child int, child_2 int, foreign key(child) references %[1]s.child(pk), foreign key(child_2) references %[1]s.child_2(pk))",
			[]string{},
		},
		{
			"three",
			"create table %[1]s.three (pk int primary key, parent int, gc int,  foreign key(parent) references %[1]s.parent(pk),foreign key(gc) references %[1]s.grandchild(pk))",
			[]string{"four"},
		},
		{
			"four",
			"create table %[1]s.four (pk int primary key, parent int, other int,  foreign key(parent) references %[1]s.parent(pk), foreign key(other) references %[1]s.three(pk))",
			[]string{"five"},
		},
		{
			"five",
			"create table %[1]s.five (pk int primary key, parent int, other int, foreign key(parent) references %[1]s.parent(pk), foreign key(other) references %[1]s.four(pk))",
			[]string{"six"},
		},
		{
			"six",
			"create table %[1]s.six (pk int primary key, parent int, other int,  foreign key(parent) references %[1]s.parent(pk), foreign key(other) references %[1]s.five(pk))",
			[]string{},
		},
		// Verify that a self-referential table returns reasonable values.
		{
			"self",
			"create table %[1]s.self (pk int primary key, this int,  foreign key(this) references %[1]s.self(pk))",
			[]string{"self_child"},
		},
		{
			"self_child",
			"create table %[1]s.self_child (pk int primary key, parent int,  this int, foreign key(parent) references %[1]s.self(pk), foreign key(this) references %[1]s.self_child(pk))",
			[]string{},
		},
	}

	ctx := fixture.Context
	pool := fixture.TargetPool
	schema := fixture.TargetSchema.Schema()

	for idx, tc := range tcs {
		sql := fmt.Sprintf(tc.schema, schema)
		log.Trace(sql)
		_, err := pool.ExecContext(ctx, sql)
		r.NoError(err, idx)
	}
	log.Info("finished creating tables")

	refs, err := getDependencyRefs(ctx, pool, schema)
	r.NoError(err)

	for _, tc := range tcs {
		parent := ident.NewTable(schema, ident.New(tc.name))

		expectedChildren := make([]ident.Table, len(tc.expectedChildren))
		for idx, childRaw := range tc.expectedChildren {
			expectedChildren[idx] = ident.NewTable(schema, ident.New(childRaw))
		}

		foundChildren := refs.GetZero(parent)
		a.Lenf(foundChildren, len(expectedChildren), "%s", parent)
	}

	// Ensure that the database query doesn't fail if there is a
	// cyclical table dependency. Dealing with this condition is left to
	// the caller.
	switch pool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		_, err = pool.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE %[1]s.cycle_a (pk int primary key);
CREATE TABLE %[1]s.cycle_b (pk int primary key, ref int references %[1]s.cycle_a);
ALTER TABLE %[1]s.cycle_a ADD COLUMN ref int references %[1]s.cycle_b;
`, fixture.TargetSchema.Schema()))
		r.NoError(err)

	case types.ProductMariaDB, types.ProductMySQL:
		_, err = pool.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %[1]s."cycle_a" (pk int primary key)`,
			fixture.TargetSchema.Schema()))
		r.NoError(err)

		_, err = pool.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %[1]s."cycle_b" (pk int primary key, ref int, foreign key(ref) references %[1]s."cycle_a"(pk))`,
			fixture.TargetSchema.Schema()))
		r.NoError(err)

		_, err = pool.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %[1]s."cycle_a" ADD COLUMN ref int, ADD FOREIGN KEY (ref) REFERENCES %[1]s."cycle_b"(pk)`,
			fixture.TargetSchema.Schema()))
		r.NoError(err)

	case types.ProductOracle:
		_, err = pool.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE %[1]s."cycle_a" (pk int primary key)`, fixture.TargetSchema.Schema()))
		r.NoError(err)

		_, err = pool.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE %[1]s."cycle_b" (pk int primary key, ref int references %[1]s."cycle_a")`,
			fixture.TargetSchema.Schema()))
		r.NoError(err)

		_, err = pool.ExecContext(ctx, fmt.Sprintf(
			`ALTER TABLE %[1]s."cycle_a" ADD (ref int references %[1]s."cycle_b")`,
			fixture.TargetSchema.Schema()))
		r.NoError(err)

	default:
		r.FailNow("unsupported product")
	}
	_, err = getDependencyRefs(ctx, pool, schema)
	r.NoError(err)
}

// TestNoDeferrableConstraints will act as a reminder if/when deferrable
// constraints are added to CRDB.
//
// https://github.com/cockroachdb/cockroach/issues/31632
func TestNoDeferrableConstraints(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	if fixture.TargetPool.Product != types.ProductCockroachDB {
		t.Skip("only for CRDB")
	}
	ctx := fixture.Context

	_, err = fixture.TargetPool.ExecContext(ctx,
		"create table x (pk int primary key, ref int references x deferrable initially deferred)")
	a.ErrorContains(err, "deferrable")
	a.ErrorContains(err, "42601")
}

// TestCrossSchemaTableReferencesPG verifies that we can handle
// cross schema constraints, in case a table with the same name
// is present in two different schemata.
func TestCrossSchemaTableReferencesPG(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	pool := fixture.TargetPool
	db, _ := fixture.TargetSchema.Split()

	// Primary must sort before secondary since we test stable output.
	primary := fixture.TargetSchema.Schema()
	var secondary ident.Schema

	switch pool.Product {
	case types.ProductPostgreSQL:
		// In Postgres we can only create schemas in the current database
		// As a workaroud, using information_schema.
		secondary, err = ident.NewSchema(db, ident.New("information_schema"))
		r.NoError(err)

	case types.ProductCockroachDB:
		secondary, err = ident.NewSchema(db, ident.New("other"))
		r.NoError(err)

		_, err = pool.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, secondary))
		r.NoError(err)

	default:
		t.Skip("only for CRDB/Postgres")
	}

	// Construct the expected schema data.
	parent := ident.NewTable(primary, ident.New("parent"))
	childPrimary := ident.NewTable(primary, ident.New("child"))
	childSecondary := ident.NewTable(secondary, ident.New("child"))

	expectMap := &ident.TableMap[[]ident.Table]{}
	tcs := []struct {
		parent   ident.Table
		children []ident.Table
	}{
		{parent, []ident.Table{childPrimary, childSecondary}},
		{childPrimary, nil},
		{childSecondary, nil},
	}
	for _, tc := range tcs {
		expectMap.Put(tc.parent, tc.children)
	}
	expectSchema := &types.SchemaData{}
	r.NoError(expectSchema.SetDependencies(expectMap))

	// Create target tables.
	q := fmt.Sprintf(`
CREATE TABLE %[1]s.parent (pk int primary key);
CREATE TABLE %[2]s.child (pk int primary key, parent int references %[1]s.parent);
CREATE TABLE %[1]s.child (pk int primary key, parent int references %[2]s.child);
`, primary, secondary)
	_, err = pool.ExecContext(ctx, q)
	r.NoError(err)

	// Execute queries and validate computed schema data.
	refs, err := getDependencyRefs(ctx, pool, primary)
	r.NoError(err)

	foundSchema := &types.SchemaData{}
	r.NoError(foundSchema.SetDependencies(refs))
	if a.Len(foundSchema.Entire.Order, len(expectSchema.Entire.Order)) {
		for idx, table := range expectSchema.Entire.Order {
			found := foundSchema.Entire.Order[idx]
			a.True(ident.Equal(table, found))
		}
	}
}
