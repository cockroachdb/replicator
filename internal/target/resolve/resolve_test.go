// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolve

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/target/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	a.NotEmpty(dbInfo.Version())
	defer cancel()

	targetDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	dataTable, err := sinktest.CreateTable(ctx, targetDB, `
CREATE TABLE %s (
  pk INT PRIMARY KEY,
  v STRING NOT NULL
)`)
	if !a.NoError(err) {
		return
	}

	// Set up the machinery in a separate DB namespace.
	metaDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	appliers, cancel := apply.NewAppliers(watchers)
	defer cancel()

	stagers := stage.NewStagers(dbInfo.Pool(), metaDB)
	tks, cancel, err := timekeeper.NewTimeKeeper(ctx, dbInfo.Pool(),
		ident.NewTable(metaDB, ident.Public, ident.New("resolved_timestamps")))
	if !a.NoError(err) {
		return
	}
	defer cancel()
	stager, err := stagers.Get(ctx, dataTable.Name())
	if !a.NoError(err) {
		return
	}

	t.Run("smoke test", func(t *testing.T) {
		pendingTable := ident.NewTable(metaDB, ident.Public, ident.New(t.Name()))
		// Boot the resolver in the sub-test, so that we know it's not
		// going to steal work from other sub-tests.
		resolvers, cancel, err := New(ctx, Config{
			Appliers:   appliers,
			MetaTable:  pendingTable,
			Pool:       dbInfo.Pool(),
			Stagers:    stagers,
			Timekeeper: tks,
			Watchers:   watchers,
		})
		if !a.NoError(err) {
			return
		}
		defer cancel()
		resolver, err := resolvers.Get(ctx, dataTable.Name().AsSchema())
		if !a.NoError(err) {
			return
		}

		a := assert.New(t)
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
		err = stager.Store(ctx, dbInfo.Pool(), muts)
		if !a.NoError(err) {
			return
		}

		// Mark some data as resolved.
		didWork, err := resolver.Mark(ctx, dbInfo.Pool(), hlc.New(expectedResolved, 0))
		if !a.NoError(err) {
			return
		}
		a.True(didWork)

		// Verify that re-marking is a no-op.
		didWork, err = resolver.Mark(ctx, dbInfo.Pool(), hlc.New(expectedResolved, 0))
		if !a.NoError(err) {
			return
		}
		a.False(didWork)

		// Verify going backwards is a no-op.
		didWork, err = resolver.Mark(ctx, dbInfo.Pool(), hlc.New(0, 0))
		if !a.NoError(err) {
			return
		}
		a.False(didWork)

		// Wait for data to be promoted.
		var promotedCount int
		for promotedCount < expectedResolved {
			time.Sleep(10 * time.Millisecond)
			err := dbInfo.Pool().QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s", dataTable),
			).Scan(&promotedCount)
			if !a.NoError(err) {
				return
			}
		}
		a.Equal(expectedResolved, promotedCount)
	})

	t.Run("check concurrent", func(t *testing.T) {
		a := assert.New(t)
		pendingTable := ident.NewTable(metaDB, ident.Public, ident.New(t.Name()))

		// Boot the resolver in the sub-test, so that we know it's not
		// going to steal work from other sub-tests.
		resolvers, cancel, err := New(ctx, Config{
			Appliers:   appliers,
			MetaTable:  pendingTable,
			Pool:       dbInfo.Pool(),
			Stagers:    stagers,
			Timekeeper: tks,
			Watchers:   watchers,
		})
		if !a.NoError(err) {
			return
		}
		defer cancel()
		// This test is going to drive some behaviors in the resolve
		// instance directly.
		resolvers.(*factory).noLoop = true

		resolver, err := resolvers.Get(ctx, dataTable.Name().AsSchema())
		if !a.NoError(err) {
			return
		}
		resolve := resolver.(*resolve)

		const markCount = 100

		// Create several entries in the meta table.
		for i := 0; i < markCount; i++ {
			didWork, err := resolve.Mark(ctx, dbInfo.Pool(), hlc.New(int64(i+1), i+1))
			a.True(didWork)
			a.NoError(err)
		}

		// Test that we correctly detect lock failures and can dequeue
		// the marked timestamps in the correct order.
		for i := 0; i < markCount; i++ {
			// Start an explicit transaction that we control.
			tx, err := dbInfo.Pool().Begin(ctx)
			if !a.NoError(err) {
				return
			}

			// Claim the next unprocessed mark.
			found, err := resolve.dequeueInTx(ctx, tx)

			// It's possible that the locks from the previous iteration
			// haven't yet been cleaned up.
			if err == errBlocked {
				i--
				// Ensure that the transaction is released.
				_ = tx.Rollback(ctx)
				continue
			}
			a.Equal(hlc.New(int64(i+1), i+1), found)
			a.NoError(err)

			// In any other transaction, we should now see a lock failure.
			found, err = resolve.dequeueInTx(ctx, dbInfo.Pool())
			a.Equal(hlc.Zero(), found)
			a.Equal(errBlocked, err)

			// Commit work.
			a.NoError(tx.Commit(ctx))
		}
	})

	// Verify that we can locate all schemas with unresolved timestamps.
	t.Run("bootstrap", func(t *testing.T) {
		a := assert.New(t)
		pendingTable := ident.NewTable(metaDB, ident.Public, ident.New(t.Name()))
		_, _, err := New(ctx, Config{
			Appliers:   appliers,
			MetaTable:  pendingTable,
			Pool:       dbInfo.Pool(),
			Stagers:    stagers,
			Timekeeper: tks,
			Watchers:   watchers,
		})
		if !a.NoError(err) {
			return
		}

		// There should be no unresolved values at this point.
		found, err := scanForTargetSchemas(ctx, dbInfo.Pool(), pendingTable)
		a.NoError(err)
		a.Empty(found)

		// Insert a fake value, ensure that it's found.
		_, err = dbInfo.Pool().Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4)", pendingTable),
			"fake_db", "fake_schema", 1234, 5678)
		a.NoError(err)

		found, err = scanForTargetSchemas(ctx, dbInfo.Pool(), pendingTable)
		a.NoError(err)
		if a.Len(found, 1) {
			a.Equal(
				ident.NewSchema(ident.New("fake_db"), ident.New("fake_schema")),
				found[0])
		}
	})
}
