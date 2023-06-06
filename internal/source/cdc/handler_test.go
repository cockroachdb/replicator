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

package cdc

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/reject"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func createFixture(t *testing.T, immediate bool) (*testFixture, base.TableInfo) {
	t.Helper()
	r := require.New(t)
	baseFixture, cancel, err := all.NewFixture()
	t.Cleanup(cancel)
	r.NoError(err)
	fixture, cancel, err := newTestFixture(baseFixture, &Config{
		MetaTableName: ident.New("resolved_timestamps"),
		BaseConfig: logical.BaseConfig{
			Immediate:  immediate,
			LoopName:   "changefeed",
			StagingDB:  baseFixture.StagingDB.Ident(),
			TargetConn: baseFixture.TargetPool.Config().ConnString(),
			TargetDB:   baseFixture.TestDB.Ident(),
		},
	})
	t.Cleanup(cancel)
	r.NoError(err)

	ctx := fixture.Context
	tableInfo, err := fixture.CreateTable(ctx,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	r.NoError(err)

	return fixture, tableInfo
}

func TestQueryHandler(t *testing.T) {
	t.Run("deferred", func(t *testing.T) { testQueryHandler(t, false) })
	t.Run("immediate", func(t *testing.T) { testQueryHandler(t, true) })
}

func testQueryHandler(t *testing.T, immediate bool) {
	t.Helper()
	fixture, tableInfo := createFixture(t, immediate)
	ctx := fixture.Context
	h := fixture.Handler
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
{"__event__": "insert", "pk" : 42, "v" : 99, "__crdb__": {"updated": "1.0"}}
{"__event__": "insert", "pk" : 99, "v" : 42, "__crdb__": {"updated": "1.0"}}
`),
		}, h.parseNdjsonQueryMutation))

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
{"__event__": "delete", "pk" : 42, "v" : null, "__crdb__": {"updated": "3.0"}}
{"__event__": "delete", "pk" : 99, "v" : null, "__crdb__": {"updated": "3.0"}}
`),
		}, h.parseNdjsonQueryMutation))

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
		schema := ident.NewSchema(
			tableInfo.Name().Database(),
			tableInfo.Name().Schema())
		table := ident.NewTable(
			tableInfo.Name().Database(),
			tableInfo.Name().Schema(),
			tableInfo.Name().Table())

		// Insert data and verify flushing.
		a.NoError(h.webhookForQuery(ctx, &request{
			target: table,
			body: strings.NewReader(`
{ "payload" : [
	{"__event__": "insert", "pk" : 42, "v" : 99, "__crdb__": {"updated": "10.0"}},
	{"__event__": "insert", "pk" : 99, "v" : 42, "__crdb__": {"updated": "10.0"}}
] }
`),
		}))

		a.NoError(h.webhookForQuery(ctx, &request{
			target: table,
			body:   strings.NewReader(`{ "__crdb__": {"resolved" : "20.0" }}`),
		}))
		a.NoError(maybeFlush(schema, hlc.New(20, 0)))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)

		// Now, delete the data.
		a.NoError(h.webhookForQuery(ctx, &request{
			target: table,
			body: strings.NewReader(`
{ "payload" : [
	{"__event__": "delete", "pk" : 42, "v" : null, "__crdb__": {"updated": "30.0"}},
	{"__event__": "delete", "pk" : 99, "v" : null, "__crdb__": {"updated": "30.0"}}
] }
`),
		}))

		a.NoError(h.webhookForQuery(ctx, &request{
			target: table,
			body:   strings.NewReader(`{ "__crdb__": {"resolved" : "40.0" }}`),
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
		}, h.parseNdjsonQueryMutation))
	})

	// Verify that an empty webhook post doesn't crash.
	t.Run("empty-webhook", func(t *testing.T) {
		a := assert.New(t)
		table := ident.NewTable(
			tableInfo.Name().Database(),
			tableInfo.Name().Schema(),
			tableInfo.Name().Table())
		a.NoError(h.webhookForQuery(ctx, &request{
			target: table,
			body:   strings.NewReader(""),
		}))
	})
}

func TestHandler(t *testing.T) {
	t.Run("deferred", func(t *testing.T) { testHandler(t, false) })
	t.Run("immediate", func(t *testing.T) { testHandler(t, true) })
}
func testHandler(t *testing.T, immediate bool) {
	t.Helper()
	r := require.New(t)
	fixture, tableInfo := createFixture(t, immediate)
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
		}, parseNdjsonMutation))

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
		}, parseNdjsonMutation))

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
		}, parseNdjsonMutation))
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
	t.Run("normal-backfill", func(t *testing.T) { testMassBackfillWithForeignKeys(t, true, 0, 1) })
	t.Run("normal-transactional", func(t *testing.T) { testMassBackfillWithForeignKeys(t, false, 0, 1) })
	t.Run("chaos-backfill", func(t *testing.T) { testMassBackfillWithForeignKeys(t, true, 0.01, 1) })
	t.Run("chaos-transactional", func(t *testing.T) { testMassBackfillWithForeignKeys(t, false, 0.01, 1) })
}

// This test simulates having multiple instances of the handler as we
// would see in a load-balanced configuration.
func TestConcurrentHandlers(t *testing.T) {
	t.Run("normal-backfill", func(t *testing.T) { testMassBackfillWithForeignKeys(t, true, 0, 3) })
	t.Run("normal-transactional", func(t *testing.T) { testMassBackfillWithForeignKeys(t, false, 0, 3) })
	t.Run("chaos-backfill", func(t *testing.T) { testMassBackfillWithForeignKeys(t, true, 0.01, 3) })
	t.Run("chaos-transactional", func(t *testing.T) { testMassBackfillWithForeignKeys(t, false, 0.01, 3) })
}

func testMassBackfillWithForeignKeys(
	t *testing.T, backfill bool, chaosProb float32, fixtureCount int,
) {
	const rowCount = 10_000
	r := require.New(t)

	baseFixture, cancel, err := all.NewFixture()
	ctx := baseFixture.Context
	r.NoError(err)
	defer cancel()

	var backfillWindow time.Duration
	if backfill {
		backfillWindow = time.Minute
	}

	fixtures := make([]*testFixture, fixtureCount)

	cancels := make([]context.CancelFunc, fixtureCount)
	defer func() {
		for _, fn := range cancels {
			if fn != nil {
				fn()
			}
		}
	}()

	for idx := range fixtures {
		fixtures[idx], cancels[idx], err = newTestFixture(baseFixture, &Config{
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
				TargetConn:         baseFixture.TargetPool.Config().ConnString(),
				TargetDB:           baseFixture.TestDB.Ident(),
			},
		})
		r.NoError(err)
	}

	loadStart := time.Now()
	log.Info("starting data load")

	parent, err := baseFixture.CreateTable(ctx,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	r.NoError(err)

	child, err := baseFixture.CreateTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (pk INT PRIMARY KEY REFERENCES %s, v INT NOT NULL)`,
		parent.Name()))
	r.NoError(err)

	grand, err := baseFixture.CreateTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (pk INT PRIMARY KEY REFERENCES %s, v INT NOT NULL)`,
		child.Name()))
	r.NoError(err)

	// h returns a random Handler instance, to simulate a loadbalancer.
	h := func() *Handler {
		return fixtures[rand.Intn(fixtureCount)].Handler
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(32)
	for i := 0; i < rowCount; i += 10 {
		i := i // Capture loop variable.
		// Stage data, but stage child data before parent data.
		for _, tbl := range []ident.Table{child.Name(), parent.Name(), grand.Name()} {
			tbl := tbl // Capture loop variable.
			eg.Go(func() error {
				if err := h().ndjson(egCtx, &request{
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
				}, parseNdjsonMutation); err != nil {
					return err
				}
				return nil
			})
		}
	}
	r.NoError(eg.Wait())

	log.Info("finished filling data in ", time.Since(loadStart).String())

	// Send a resolved timestamp that needs to dequeue many rows.
	r.NoError(h().resolved(ctx, &request{
		target:    parent.Name(),
		timestamp: hlc.New(rowCount+1, 0),
	}))

	// Wait for rows to appear.
	for _, tbl := range []ident.Table{parent.Name(), child.Name(), grand.Name()} {
		for {
			count, err := base.GetRowCount(ctx, baseFixture.TargetPool, tbl)
			r.NoError(err)
			log.Infof("saw %d of %d rows in %s", count, rowCount, tbl)
			if count == rowCount {
				break
			}
			time.Sleep(time.Second)
		}
	}
}
