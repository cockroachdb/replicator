// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"strings"
	"testing"

	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/backend/apply"
	"github.com/cockroachdb/cdc-sink/internal/backend/mutation"
	"github.com/cockroachdb/cdc-sink/internal/backend/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/backend/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/backend/timestamp"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestHandler(t *testing.T) {
	t.Run("deferred", func(t *testing.T) { testHandler(t, false) })
	t.Run("immediate", func(t *testing.T) { testHandler(t, true) })
}

func testHandler(t *testing.T, immediate bool) {
	t.Helper()
	a := assert.New(t)

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	tableInfo, err := sinktest.CreateTable(ctx, dbName,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	if !a.NoError(err) {
		return
	}

	swapper, err := timestamp.New(ctx, dbInfo.Pool(), Resolved)
	if !a.NoError(err) {
		return
	}

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	appliers, cancel := apply.New(watchers)
	defer cancel()

	h := &Handler{
		Appliers:  appliers,
		Immediate: immediate,
		Pool:      dbInfo.Pool(),
		Stores:    mutation.New(dbInfo.Pool(), ident.StagingDB),
		Swapper:   swapper,
		Watchers:  watchers,
	}

	t.Run("bulkStyleEndpoints", func(t *testing.T) {
		a := assert.New(t)

		a.NoError(h.ndjson(ctx,
			ndjsonURL{
				target: tableInfo.Name(),
			},
			strings.NewReader(`
{ "after" : { "pk" : 42, "v" : 99 }, "key" : [ 42 ], "updated" : "1.0" }
{ "after" : { "pk" : 99, "v" : 42 }, "key" : [ 99 ], "updated" : "1.0" }
`)))

		a.NoError(h.resolved(ctx, resolvedURL{
			target:    ident.NewSchema(tableInfo.Name().Database(), tableInfo.Name().Schema()),
			timestamp: hlc.New(2, 0),
		}))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)

		// Now, delete the data.

		a.NoError(h.ndjson(ctx,
			ndjsonURL{
				target: tableInfo.Name(),
			},
			strings.NewReader(`
{ "after" : null, "key" : [ 42 ], "updated" : "3.0" }
{ "key" : [ 99 ], "updated" : "3.0" }
`)))

		a.NoError(h.resolved(ctx, resolvedURL{
			target:    ident.NewSchema(tableInfo.Name().Database(), tableInfo.Name().Schema()),
			timestamp: hlc.New(5, 0),
		}))

		ct, err = tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(0, ct)
	})

	t.Run("webhookEndpoints", func(t *testing.T) {
		a := assert.New(t)
		schema := ident.NewSchema(tableInfo.Name().Database(), tableInfo.Name().Schema())
		a.NoError(h.webhook(ctx,
			webhookURL{schema},
			strings.NewReader(fmt.Sprintf(`
{ "payload" : [
  { "after" : { "pk" : 42, "v" : 99 }, "key" : [ 42 ], "topic" : %[1]s, "updated" : "10.0" },
  { "after" : { "pk" : 99, "v" : 42 }, "key" : [ 99 ], "topic" : %[1]s, "updated" : "10.0" }
] }
`, tableInfo.Name().Table()))))

		a.NoError(h.webhook(ctx,
			webhookURL{schema},
			strings.NewReader(`{ "resolved" : "20.0" }`)))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)
	})

	t.Run("empty-ndjson", func(t *testing.T) {
		a := assert.New(t)
		a.NoError(h.ndjson(ctx,
			ndjsonURL{
				target: tableInfo.Name(),
			},
			strings.NewReader("")))
	})

	t.Run("resolved-goes-backwards", func(t *testing.T) {
		a := assert.New(t)

		a.NoError(h.resolved(ctx, resolvedURL{
			target:    ident.NewSchema(tableInfo.Name().Database(), tableInfo.Name().Schema()),
			timestamp: hlc.New(50000, 0),
		}))
		err := h.resolved(ctx, resolvedURL{
			target:    ident.NewSchema(tableInfo.Name().Database(), tableInfo.Name().Schema()),
			timestamp: hlc.New(25, 0),
		})
		if immediate {
			a.NoError(err)
		} else if a.Error(err) {
			a.Contains(err.Error(), "backwards")
		}
	})
}
