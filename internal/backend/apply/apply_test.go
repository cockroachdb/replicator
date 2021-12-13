// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/backend/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/backend/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/stretchr/testify/assert"
)

// This test inserts and deletes rows from a trivial table.
func TestApply(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDb(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	type Payload struct {
		Pk0 int    `json:"pk0"`
		Pk1 string `json:"pk1"`
	}
	tbl, err := sinktest.CreateTable(ctx, dbName,
		"CREATE TABLE %s (pk0 INT, pk1 STRING, PRIMARY KEY (pk0,pk1))")
	if !a.NoError(err) {
		return
	}

	watcher, err := watchers.Get(ctx, dbName)
	if !a.NoError(err) {
		return
	}

	app, cancel, err := newApply(watcher, tbl.Name())
	if !a.NoError(err) {
		return
	}
	defer cancel()

	t.Log(app.mu.sql.delete)
	t.Log(app.mu.sql.upsert)

	t.Run("smoke", func(t *testing.T) {
		a := assert.New(t)
		count := 3 * batches.Size()
		adds := make([]sinktypes.Mutation, count)
		dels := make([]sinktypes.Mutation, count)
		for i := range adds {
			p := Payload{Pk0: i, Pk1: fmt.Sprintf("X%dX", i)}
			bytes, err := json.Marshal(p)
			a.NoError(err)
			adds[i] = sinktypes.Mutation{Data: bytes}

			bytes, err = json.Marshal([]interface{}{p.Pk0, p.Pk1})
			a.NoError(err)
			dels[i] = sinktypes.Mutation{Key: bytes}
		}

		// Verify insertion
		a.NoError(app.Apply(ctx, dbInfo.Pool(), adds))
		ct, err := tbl.RowCount(ctx)
		a.Equal(count, ct)
		a.NoError(err)

		// Verify that they can be deleted.
		a.NoError(app.Apply(ctx, dbInfo.Pool(), dels))
		ct, err = tbl.RowCount(ctx)
		a.Equal(0, ct)
		a.NoError(err)
	})

	// Verify unexpected incoming column
	t.Run("unexpected", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, dbInfo.Pool(), []sinktypes.Mutation{
			{
				Data: []byte(`{"pk0":1, "pk1":0, "no_good":true}`),
			},
		}); a.Error(err) {
			t.Log(err.Error())
			a.Contains(err.Error(), "unexpected columns [no_good]")
		}
	})

	t.Run("missing_key_upsert", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, dbInfo.Pool(), []sinktypes.Mutation{
			{
				Data: []byte(`{"pk0":1}`),
			},
		}); a.Error(err) {
			t.Log(err.Error())
			a.Contains(err.Error(), "missing PK column pk1")
		}
	})

	t.Run("missing_key_delete_too_few", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, dbInfo.Pool(), []sinktypes.Mutation{
			{
				Key: []byte(`[1]`),
			},
		}); a.Error(err) {
			t.Log(err.Error())
			a.Contains(err.Error(), "received 1 expect 2")
		}
	})

	t.Run("missing_key_delete_too_many", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, dbInfo.Pool(), []sinktypes.Mutation{
			{
				Key: []byte(`[1, 2, 3]`),
			},
		}); a.Error(err) {
			t.Log(err.Error())
			a.Contains(err.Error(), "received 3 expect 2")
		}
	})
}
