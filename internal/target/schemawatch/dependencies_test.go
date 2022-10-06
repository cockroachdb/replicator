// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemawatch_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDependencyOrder(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, cancel, err := sinktest.NewFixture()
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
			"create table %[1]s.parent (pk uuid primary key)",
		},
		{
			"parent_2",
			0,
			"create table %[1]s.parent_2 (pk uuid primary key)",
		},
		{
			"unreferenced",
			0,
			"create table %[1]s.unreferenced (pk uuid primary key)",
		},
		{
			"child",
			1,
			"create table %[1]s.child (pk uuid primary key, parent uuid references %[1]s.parent)",
		},
		{
			"child_2",
			1,
			"create table %[1]s.child_2 (pk uuid primary key, parent_2 uuid references %[1]s.parent_2)",
		},
		{
			"grandchild",
			2,
			"create table %[1]s.grandchild (pk uuid primary key, child uuid references %[1]s.child)",
		},
		{
			"grandchild_2",
			2,
			"create table %[1]s.grandchild_2 (pk uuid primary key, child_2 uuid references %[1]s.child_2)",
		},
		{
			"grandchild_multi",
			2,
			"create table %[1]s.grandchild_multi (pk uuid primary key, child uuid references %[1]s.child, child_2 uuid references %[1]s.child_2)",
		},
		{
			"three",
			3,
			"create table %[1]s.three (pk uuid primary key, parent uuid references %[1]s.parent, gc uuid references %[1]s.grandchild)",
		},
		{
			"four",
			4,
			"create table %[1]s.four (pk uuid primary key, parent uuid references %[1]s.parent, three uuid references %[1]s.three)",
		},
		{
			"five",
			5,
			"create table %[1]s.five (pk uuid primary key, parent uuid references %[1]s.parent, three uuid references %[1]s.four)",
		},
		{
			"six",
			6,
			"create table %[1]s.six (pk uuid primary key, parent uuid references %[1]s.parent, three uuid references %[1]s.five)",
		},
		// Verify that a self-referential table returns reasonable values.
		{
			"self",
			0,
			"create table %[1]s.self (pk uuid primary key, ref uuid references %[1]s.self)",
		},
		{
			"self_child",
			1,
			"create table %[1]s.self_child (pk uuid primary key, self uuid references %[1]s.self, self_child uuid references %[1]s.self_child)",
		},
	}
	expected := make(map[string]int, len(tcs))
	for _, tc := range tcs {
		expected[tc.name] = tc.order
	}

	ctx := fixture.Context
	pool := fixture.Pool

	for idx, tc := range tcs {
		sql := fmt.Sprintf(tc.schema, fixture.TestDB.Ident())
		_, err := pool.Exec(ctx, sql)
		r.NoError(err, idx)
	}

	r.NoError(fixture.Watcher.Refresh(ctx, pool))
	snap := fixture.Watcher.Snapshot(ident.NewSchema(fixture.TestDB.Ident(), ident.Public))

	tableCount := 0
	found := make(map[string]int, len(expected))
	for idx, tables := range snap.Order {
		for _, table := range tables {
			found[table.Table().Raw()] = idx
			tableCount++
		}
	}

	a.Equal(len(tcs), tableCount)
	a.Equal(expected, found)

	// Ensure that we fail in a useful manner if there is a reference cycle.
	_, err = pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE %[1]s.cycle_a (pk uuid primary key);
CREATE TABLE %[1]s.cycle_b (pk uuid primary key, ref uuid references %[1]s.cycle_a);
ALTER TABLE %[1]s.cycle_a ADD COLUMN ref uuid references %[1]s.cycle_b;
`, fixture.TestDB.Ident()))
	r.NoError(err)
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

	fixture, cancel, err := sinktest.NewBaseFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context

	_, err = fixture.Pool.Exec(ctx,
		"create table x (pk uuid primary key, ref uuid references x deferrable initially deferred)")
	a.ErrorContains(err, "deferrable")
	a.ErrorContains(err, "42601")
}
