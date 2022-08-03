// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

// Verify round-trip through persistence code.
func TestPersistenceRoundTrip(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	cfgs := fixture.Configs

	tbl := ident.NewTable(ident.New("db"), ident.New("public"), ident.New("target"))

	watch, cancel := cfgs.Watch(tbl)
	defer cancel()
	// Helper to perform a timed read from the watch channel.
	readWatch := func() *apply.Config {
		select {
		case ret := <-watch:
			return ret
		case <-time.After(time.Second):
			a.Fail("timed out waiting for watch")
			return &apply.Config{}
		}
	}

	// We should see data immediately.
	a.True(readWatch().IsZero())

	cfg := &apply.Config{
		CASColumns: []ident.Ident{
			ident.New("cas1"),
			ident.New("cas2"),
		},
		Deadlines: types.Deadlines{
			ident.New("dl1"): time.Second,
			ident.New("dl2"): 2 * time.Second,
		},
		Exprs: map[ident.Ident]string{
			ident.New("expr1"): "1 + $0",
			ident.New("expr2"): "2 + $0",
		},
		Extras: ident.New("extras"),
		Ignore: map[ident.Ident]bool{
			ident.New("ignore1"): true,
			ident.New("ignore2"): true,
		},
		SourceNames: map[ident.Ident]ident.Ident{
			ident.New("rename1"): ident.New("renamed1"),
			ident.New("rename2"): ident.New("renamed2"),
		},
	}
	a.Equal(cfg, cfg.Copy())

	// Check zero value.
	a.True(cfgs.Get(tbl).IsZero())

	// Verify that we can store the data.
	tx, err := fixture.Pool.Begin(ctx)
	a.NoError(err)
	a.NoError(cfgs.Store(ctx, tx, tbl, cfg))
	a.NoError(tx.Commit(ctx))

	// Reload with persisted data.
	changed, err := fixture.Configs.Refresh(ctx)
	a.True(changed)
	a.NoError(err)

	// Verify no-change behavior.
	changed, err = fixture.Configs.Refresh(ctx)
	a.False(changed)
	a.NoError(err)

	// Verify that the data is equal.
	found := cfgs.Get(tbl)
	a.Equal(cfg, found)

	bytes, err := json.Marshal(cfgs.GetAll())
	if a.NoError(err) {
		t.Log(string(bytes))
	}

	// Verify updated data from the watch.
	a.Equal(cfg, readWatch())

	// Replace the data with an empty configuration, this will wind
	// up deleting the config rows.
	a.NoError(cfgs.Store(ctx, fixture.Pool, tbl, &apply.Config{}))
	changed, err = fixture.Configs.Refresh(ctx)
	a.True(changed)
	a.NoError(err)
	a.True(cfgs.Get(tbl).IsZero())
	a.True(readWatch().IsZero())
}

func TestZero(t *testing.T) {
	a := assert.New(t)

	a.True(apply.NewConfig().IsZero())
}
