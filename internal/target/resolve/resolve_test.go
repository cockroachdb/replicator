// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolve_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/resolve"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestSmoke(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	pool := fixture.Pool

	// Create a table to receive the staged mutations.
	tableInfo, err := fixture.CreateTable(ctx, `
CREATE TABLE %s (
  pk INT PRIMARY KEY,
  v STRING NOT NULL
)`)
	if !a.NoError(err) {
		return
	}
	dataTable := tableInfo.Name()

	stager, err := fixture.Stagers.Get(ctx, dataTable)
	if !a.NoError(err) {
		return
	}
	resolver, err := fixture.Resolvers.Get(ctx, dataTable.AsSchema())
	if !a.NoError(err) {
		return
	}

	// We want to leave some mutations dangling, to verify bounds
	// behavior when dequeuing.
	const count = 1000
	const expectedResolved = 900

	// Stage some data.
	muts := make([]types.Mutation, count)
	for i := range muts {
		muts[i] = types.Mutation{
			Data: []byte(fmt.Sprintf(`{ "pk": %d, "v": "number %d" }`, i, i)),
			Key:  []byte(fmt.Sprintf(`[ %d ]`, i)),
			Time: hlc.New(int64(i), i),
		}
	}
	err = stager.Store(ctx, pool, muts)
	if !a.NoError(err) {
		return
	}

	// Mark some data as resolved.
	didWork, err := resolver.Mark(ctx, pool, hlc.New(expectedResolved, 0))
	if !a.NoError(err) {
		return
	}
	a.True(didWork)

	// Verify that re-marking is a no-op.
	didWork, err = resolver.Mark(ctx, pool, hlc.New(expectedResolved, 0))
	if !a.NoError(err) {
		return
	}
	a.False(didWork)

	// Verify going backwards is a no-op.
	didWork, err = resolver.Mark(ctx, pool, hlc.New(0, 0))
	if !a.NoError(err) {
		return
	}
	a.False(didWork)

	// Wait for data to be promoted.
	var promotedCount int
	for promotedCount < expectedResolved {
		time.Sleep(10 * time.Millisecond)
		err := pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s", dataTable),
		).Scan(&promotedCount)
		if !a.NoError(err) {
			return
		}
	}
	a.Equal(expectedResolved, promotedCount)
}

func TestBootstrap(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	metaTable := fixture.MetaTable.Table()
	pool := fixture.Pool

	// There should be no unresolved values at this point.
	found, err := resolve.ScanForTargetSchemas(ctx, pool, metaTable)
	a.NoError(err)
	a.Empty(found)

	// Insert a fake value, ensure that it's found.
	_, err = pool.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4)", metaTable),
		"fake_db", "fake_schema", 1234, 5678)
	a.NoError(err)

	found, err = resolve.ScanForTargetSchemas(ctx, pool, metaTable)
	a.NoError(err)
	if a.Len(found, 1) {
		a.Equal(
			ident.NewSchema(ident.New("fake_db"), ident.New("fake_schema")),
			found[0])
	}
}
