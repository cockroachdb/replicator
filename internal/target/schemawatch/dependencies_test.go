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

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/cmap"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDependencyOrder(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, cancel, err := all.NewFixture()
	r.NoError(err)
	defer cancel()

	tcs := []struct {
		name   string
		order  int
		schema string
	}{
		{
			"parent",
			0,
			"create table %[1]s.parent (pk int primary key)",
		},
		{
			"parent_2",
			0,
			"create table %[1]s.parent_2 (pk int primary key)",
		},
		{
			"unreferenced",
			0,
			"create table %[1]s.unreferenced (pk int primary key)",
		},
		{
			"child",
			1,
			"create table %[1]s.child (pk int primary key, parent int references %[1]s.parent)",
		},
		{
			"child_2",
			1,
			"create table %[1]s.child_2 (pk int primary key, parent_2 int references %[1]s.parent_2)",
		},
		{
			"grandchild",
			2,
			"create table %[1]s.grandchild (pk int primary key, child int references %[1]s.child)",
		},
		{
			"grandchild_2",
			2,
			"create table %[1]s.grandchild_2 (pk int primary key, child_2 int references %[1]s.child_2)",
		},
		{
			"grandchild_multi",
			2,
			"create table %[1]s.grandchild_multi (pk int primary key, child int references %[1]s.child, child_2 int references %[1]s.child_2)",
		},
		{
			"three",
			3,
			"create table %[1]s.three (pk int primary key, parent int references %[1]s.parent, gc int references %[1]s.grandchild)",
		},
		{
			"four",
			4,
			"create table %[1]s.four (pk int primary key, parent int references %[1]s.parent, three int references %[1]s.three)",
		},
		{
			"five",
			5,
			"create table %[1]s.five (pk int primary key, parent int references %[1]s.parent, three int references %[1]s.four)",
		},
		{
			"six",
			6,
			"create table %[1]s.six (pk int primary key, parent int references %[1]s.parent, three int references %[1]s.five)",
		},
		// Verify that a self-referential table returns reasonable values.
		{
			"self",
			0,
			"create table %[1]s.self (pk int primary key, ref int references %[1]s.self)",
		},
		{
			"self_child",
			1,
			"create table %[1]s.self_child (pk int primary key, self int references %[1]s.self, self_child int references %[1]s.self_child)",
		},
	}
	expected := &ident.Map[int]{}
	for _, tc := range tcs {
		expected.Put(ident.New(tc.name), tc.order)
	}

	ctx := fixture.Context
	pool := fixture.TargetPool

	for idx, tc := range tcs {
		sql := fmt.Sprintf(tc.schema, fixture.TargetSchema.Schema())
		_, err := pool.ExecContext(ctx, sql)
		r.NoError(err, idx)
	}

	r.NoError(fixture.Watcher.Refresh(ctx, pool))
	snap := fixture.Watcher.Get()

	tableCount := 0
	found := &ident.Map[int]{}
	for idx, tables := range snap.Order {
		for _, table := range tables {
			found.Put(table.Table(), idx)
			tableCount++
		}
	}

	a.Equal(len(tcs), tableCount)
	a.True(expected.Equal(found, cmap.Comparator[int]()))

	// Ensure that we fail in a useful manner if there is a reference cycle.
	switch pool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		_, err = pool.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE %[1]s.cycle_a (pk int primary key);
CREATE TABLE %[1]s.cycle_b (pk int primary key, ref int references %[1]s.cycle_a);
ALTER TABLE %[1]s.cycle_a ADD COLUMN ref int references %[1]s.cycle_b;
`, fixture.TargetSchema.Schema()))
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
			`ALTER TABLE %[1]s."cycle_a" ADD (ref int references %[1]s."cycle_b")`, fixture.TargetSchema.Schema()))
		r.NoError(err)

	default:
		r.FailNow("unsupported product")
	}
	err = fixture.Watcher.Refresh(ctx, pool)
	a.ErrorContains(err, "cycle_a")
	a.ErrorContains(err, "cycle_b")
}

// TestNoDeferrableConstraints will act as a reminder if/when deferrable
// constraints are added to CRDB.
//
// https://github.com/cockroachdb/cockroach/issues/31632
func TestNoDeferrableConstraints(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

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

	fixture, cancel, err := all.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	pool := fixture.TargetPool
	db, _ := fixture.TargetSchema.Split()
	switch pool.Product {
	case types.ProductPostgreSQL:
		// In Postgres we can only create schemas in the current database
		// As a workaroud, using information_schema.
		stm := fmt.Sprintf(`
		CREATE TABLE %[1]s.parent (pk int primary key);
		CREATE TABLE %[2]s.child (pk int primary key, parent int references %[1]s.parent);
		CREATE TABLE %[1]s.child (pk int primary key, parent int references %[2]s.child);
		`, fixture.TargetSchema, "information_schema")
		_, err = pool.ExecContext(ctx, stm)
		r.NoError(err)
	case types.ProductCockroachDB:

		other, err := ident.NewSchema(db, ident.New("other"))
		r.NoError(err)
		stm := fmt.Sprintf(`
		CREATE SCHEMA %[2]s;
		CREATE TABLE %[1]s.parent (pk int primary key);
		CREATE TABLE %[2]s.child (pk int primary key, parent int references %[1]s.parent);
		CREATE TABLE %[1]s.child (pk int primary key, parent int references %[2]s.child);
		`, fixture.TargetSchema, other)
		_, err = pool.ExecContext(ctx, stm)
		r.NoError(err)
	default:
		t.Skip("only for CRDB/Postgres")
	}
	r.NoError(fixture.Watcher.Refresh(ctx, pool))
	snap := fixture.Watcher.Get()
	for idx, tables := range snap.Order {
		switch idx {
		case 0:
			r.Equal(1, len(tables))
			a.True(ident.Equal(ident.New("parent"), tables[0].Table()))
			a.True(ident.Equal(fixture.TargetSchema, tables[0].Schema()))
		case 2:
			r.Equal(1, len(tables))
			a.True(ident.Equal(ident.New("child"), tables[0].Table()))
			a.True(ident.Equal(fixture.TargetSchema, tables[0].Schema()))
		}
	}
}
