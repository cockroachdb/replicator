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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target/auth/reject"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler(t *testing.T) {
	t.Run("deferred", func(t *testing.T) { testHandler(t, false) })
	t.Run("immediate", func(t *testing.T) { testHandler(t, true) })
}

func testHandler(t *testing.T, immediate bool) {
	t.Helper()
	r := require.New(t)

	baseFixture, cancel, err := sinktest.NewFixture()
	r.NoError(err)
	defer cancel()

	fixture, cancel, err := newTestFixture(baseFixture, &Config{
		MetaTableName: ident.New("resolved_timestamps"),
		BaseConfig: logical.BaseConfig{
			Immediate:  immediate,
			LoopName:   "changefeed",
			StagingDB:  baseFixture.StagingDB.Ident(),
			TargetConn: baseFixture.Pool.Config().ConnString(),
			TargetDB:   baseFixture.TestDB.Ident(),
		},
	})
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	h := fixture.Handler

	tableInfo, err := fixture.CreateTable(ctx,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	r.NoError(err)

	// In async mode, we want to reach into the implementation to
	// force the marked, resolved timestamp to be operated on.
	maybeFlush := func(target ident.Schematic, expect hlc.Time) error {
		if immediate {
			return nil
		}
		resolver, err := h.Resolvers.get(ctx, target.AsSchema())
		if err != nil {
			return err
		}
		waitFor := &resolvedStamp{CommittedTime: expect}
		resolver.fastWakeup <- struct{}{}
		for resolver.loop.GetConsistentPoint().Less(waitFor) && ctx.Err() == nil {
			time.Sleep(time.Millisecond)
		}
		return nil
	}

	// Validate the "classic" endpoints where files containing
	// newline-delimited json and resolved timestamp markers are
	// uploaded.
	//
	// The inner methods of Handler are called directly, to avoid the
	// need to cook up fake http.Request instances.
	t.Run("bulkStyleEndpoints", func(t *testing.T) {
		a := assert.New(t)

		// Stage two lines of data.
		a.NoError(h.ndjson(ctx, &request{
			target: tableInfo.Name(),
			body: strings.NewReader(`
{ "after" : { "pk" : 42, "v" : 99 }, "key" : [ 42 ], "updated" : "1.0" }
{ "after" : { "pk" : 99, "v" : 42 }, "key" : [ 99 ], "updated" : "1.0" }
`),
		}))

		// Send the resolved timestamp request.
		a.NoError(h.resolved(ctx, &request{
			target:    tableInfo.Name(),
			timestamp: hlc.New(2, 0),
		}))
		a.NoError(maybeFlush(tableInfo.Name(), hlc.New(2, 0)))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)

		// Now, delete the data.
		a.NoError(h.ndjson(ctx, &request{
			target: tableInfo.Name(),
			body: strings.NewReader(`
{ "after" : null, "key" : [ 42 ], "updated" : "3.0" }
{ "key" : [ 99 ], "updated" : "3.0" }
`),
		}))

		a.NoError(h.resolved(ctx, &request{
			target:    tableInfo.Name(),
			timestamp: hlc.New(5, 0),
		}))
		a.NoError(maybeFlush(tableInfo.Name(), hlc.New(5, 0)))

		ct, err = tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(0, ct)
	})

	// This test validates the new webhook-https:// scheme introduced
	// in CockroachDB v21.2. We insert rows and delete them.
	t.Run("webhookEndpoints", func(t *testing.T) {
		a := assert.New(t)
		schema := ident.NewSchema(tableInfo.Name().Database(), tableInfo.Name().Schema())

		// Insert data and verify flushing.
		a.NoError(h.webhook(ctx, &request{
			target: schema,
			body: strings.NewReader(fmt.Sprintf(`
{ "payload" : [
  { "after" : { "pk" : 42, "v" : 99 }, "key" : [ 42 ], "topic" : %[1]s, "updated" : "10.0" },
  { "after" : { "pk" : 99, "v" : 42 }, "key" : [ 99 ], "topic" : %[1]s, "updated" : "10.0" }
] }
`, tableInfo.Name().Table())),
		}))

		a.NoError(h.webhook(ctx, &request{
			target: schema,
			body:   strings.NewReader(`{ "resolved" : "20.0" }`),
		}))
		a.NoError(maybeFlush(schema, hlc.New(20, 0)))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)

		// Now, delete the data.
		a.NoError(h.webhook(ctx, &request{
			target: schema,
			body: strings.NewReader(fmt.Sprintf(`
{ "payload" : [
  { "key" : [ 42 ], "topic" : %[1]s, "updated" : "30.0" },
  { "after" : null, "key" : [ 99 ], "topic" : %[1]s, "updated" : "30.0" }
] }
`, tableInfo.Name().Table())),
		}))

		a.NoError(h.webhook(ctx, &request{
			target: schema,
			body:   strings.NewReader(`{ "resolved" : "40.0" }`),
		}))
		a.NoError(maybeFlush(schema, hlc.New(40, 0)))

		ct, err = tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(0, ct)
	})

	// Verify that an empty post doesn't crash.
	t.Run("empty-ndjson", func(t *testing.T) {
		a := assert.New(t)
		a.NoError(h.ndjson(ctx, &request{
			target: tableInfo.Name(),
			body:   strings.NewReader(""),
		}))
	})

	// Verify that an empty webhook post doesn't crash.
	t.Run("empty-webhook", func(t *testing.T) {
		a := assert.New(t)
		a.NoError(h.webhook(ctx, &request{
			target: ident.NewSchema(tableInfo.Name().Database(), tableInfo.Name().Schema()),
			body:   strings.NewReader(""),
		}))
	})
}

func TestRejectedAuth(t *testing.T) {
	// Verify that auth checks don't require other services.
	h := &Handler{
		Authenticator: reject.New(),
	}

	tcs := []struct {
		path string
		code int
	}{
		{"/targetDB/targetSchema/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-test_table-1.ndjson", http.StatusUnauthorized},
		{"/test/public/2020-04-04/202004042351304139680000000000000.RESOLVED", http.StatusUnauthorized},
		{"/some/schema", http.StatusUnauthorized}, // webhook
		{"/", http.StatusNotFound},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			req := httptest.NewRequest("POST", tc.path, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)
			a.Equal(tc.code, w.Code)
		})
	}
}

// This test adds a large number of rows that have FK dependencies, in
// excess of a number that should be processed as a single transaction.
func TestWithForeignKeys(t *testing.T) {
	t.Run("normal-backfill", func(t *testing.T) { testMassBackfillWithForeignKeys(t, true, 0) })
	t.Run("normal-transactional", func(t *testing.T) { testMassBackfillWithForeignKeys(t, false, 0) })
	t.Run("chaos-backfill", func(t *testing.T) { testMassBackfillWithForeignKeys(t, true, 0.01) })
	t.Run("chaos-transactional", func(t *testing.T) { testMassBackfillWithForeignKeys(t, false, 0.01) })
}

func testMassBackfillWithForeignKeys(t *testing.T, backfill bool, chaosProb float32) {
	const rowCount = 10_000
	r := require.New(t)

	baseFixture, cancel, err := sinktest.NewFixture()
	r.NoError(err)
	defer cancel()

	var backfillWindow time.Duration
	if backfill {
		backfillWindow = time.Minute
	}

	fixture, cancel, err := newTestFixture(baseFixture, &Config{
		IdealFlushBatchSize: 123, // Pick weird batch sizes.
		SelectBatchSize:     587,
		MetaTableName:       ident.New("resolved_timestamps"),
		BaseConfig: logical.BaseConfig{
			ApplyTimeout:       time.Second,
			BackfillWindow:     backfillWindow,
			ChaosProb:          chaosProb,
			FanShards:          16,
			ForeignKeysEnabled: true,
			LoopName:           "changefeed",
			StagingDB:          baseFixture.StagingDB.Ident(),
			RetryDelay:         time.Nanosecond,
			TargetConn:         baseFixture.Pool.Config().ConnString(),
			TargetDB:           baseFixture.TestDB.Ident(),
		},
	})
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	h := fixture.Handler

	parent, err := fixture.CreateTable(ctx,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	r.NoError(err)

	child, err := fixture.CreateTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (pk INT PRIMARY KEY REFERENCES %s, v INT NOT NULL)`,
		parent.Name()))
	r.NoError(err)

	grand, err := fixture.CreateTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (pk INT PRIMARY KEY REFERENCES %s, v INT NOT NULL)`,
		child.Name()))
	r.NoError(err)

	for i := 0; i < rowCount; i += 10 {
		// Stage data, but stage child data before parent data.
		for _, tbl := range []ident.Table{child.Name(), parent.Name(), grand.Name()} {
			r.NoError(h.ndjson(ctx, &request{
				target: tbl,
				body: strings.NewReader(fmt.Sprintf(`
{ "after" : { "pk" : %[1]d, "v" : %[1]d }, "key" : [ %[1]d ], "updated" : "%[1]d.0" }
{ "after" : { "pk" : %[2]d, "v" : %[2]d }, "key" : [ %[2]d ], "updated" : "%[2]d.0" }
{ "after" : { "pk" : %[3]d, "v" : %[3]d }, "key" : [ %[3]d ], "updated" : "%[3]d.0" }
{ "after" : { "pk" : %[4]d, "v" : %[4]d }, "key" : [ %[4]d ], "updated" : "%[4]d.0" }
{ "after" : { "pk" : %[5]d, "v" : %[5]d }, "key" : [ %[5]d ], "updated" : "%[5]d.0" }
{ "after" : { "pk" : %[6]d, "v" : %[6]d }, "key" : [ %[6]d ], "updated" : "%[6]d.0" }
{ "after" : { "pk" : %[7]d, "v" : %[7]d }, "key" : [ %[7]d ], "updated" : "%[7]d.0" }
{ "after" : { "pk" : %[8]d, "v" : %[8]d }, "key" : [ %[8]d ], "updated" : "%[8]d.0" }
{ "after" : { "pk" : %[9]d, "v" : %[9]d }, "key" : [ %[9]d ], "updated" : "%[9]d.0" }
{ "after" : { "pk" : %[10]d, "v" : %[10]d }, "key" : [ %[10]d ], "updated" : "%[10]d.0" }`,
					i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10)),
			}))
		}
	}
	log.Info("finished filling data")

	// Send a resolved timestamp that needs to dequeue many rows.
	r.NoError(h.resolved(ctx, &request{
		target:    parent.Name(),
		timestamp: hlc.New(rowCount+1, 0),
	}))

	// Wait for rows to appear.
	for _, tbl := range []ident.Table{parent.Name(), child.Name(), grand.Name()} {
		for {
			count, err := sinktest.GetRowCount(ctx, fixture.Pool, tbl)
			r.NoError(err)
			log.Infof("saw %d of %d rows in %s", count, rowCount, tbl)
			if count == rowCount {
				break
			}
			time.Sleep(time.Second)
		}
	}
}
