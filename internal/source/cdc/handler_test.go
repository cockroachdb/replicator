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
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/conveyor"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/scripttest"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/auth/reject"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/merge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixtureConfig struct {
	discard   bool
	immediate bool
	script    bool
}

func TestHandler(t *testing.T) {
	tcs := []struct {
		name string
		cfg  *fixtureConfig
	}{
		{"deferred", &fixtureConfig{}},
		{"deferred-script", &fixtureConfig{script: true}},
		{"immediate", &fixtureConfig{immediate: true}},
		{"immediate-script", &fixtureConfig{immediate: true, script: true}},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s-feed", tc.name), func(t *testing.T) { testHandler(t, tc.cfg) })
		t.Run(fmt.Sprintf("%s-query", tc.name), func(t *testing.T) { testQueryHandler(t, tc.cfg) })
	}
}

func createFixture(
	t *testing.T, htc *fixtureConfig,
) (*testFixture, base.TableInfo[*types.TargetPool]) {
	t.Helper()
	r := require.New(t)
	baseFixture, err := all.NewFixture(t)
	r.NoError(err)

	// Ensure that the dispatch and mapper functions must have been
	// called by using a column name that's only known to the mapper
	// function. We set this to a type large enough to store a 64-bit
	// number so we can test for numeric type truncation.
	var schema string
	if htc.script {
		schema = `CREATE TABLE %s (pk INT PRIMARY KEY, v_mapped BIGINT NOT NULL)`
	} else {
		schema = `CREATE TABLE %s (pk INT PRIMARY KEY, v BIGINT NOT NULL)`
	}
	if baseFixture.TargetPool.Product == types.ProductOracle {
		schema = strings.ReplaceAll(schema, "BIGINT", "NUMBER(20,0)")
	}

	ctx := baseFixture.Context
	tableInfo, err := baseFixture.CreateTargetTable(ctx, schema)
	r.NoError(err)

	cfg := &Config{
		ConveyorConfig: conveyor.Config{
			BestEffortWindow: math.MaxInt64,
			Immediate:        htc.immediate,
		},
		Discard: htc.discard,
		SequencerConfig: sequencer.Config{
			RetireOffset:    time.Hour, // Enable post-hoc inspection.
			QuiescentPeriod: time.Second,
		},
	}

	if htc.script {
		cfg.ScriptConfig = script.Config{
			FS:       scripttest.ScriptFSFor(tableInfo.Name()),
			MainPath: "/testdata/logical_test.ts",
		}
	}
	r.NoError(cfg.Preflight())
	fixture, err := newTestFixture(baseFixture, cfg)
	r.NoError(err)

	return fixture, tableInfo
}

func testQueryHandler(t *testing.T, htc *fixtureConfig) {
	t.Parallel()
	t.Helper()
	fixture, tableInfo := createFixture(t, htc)
	ctx := fixture.Context
	h := fixture.Handler
	// In async mode, we want to reach into the implementation to
	// force the marked, resolved timestamp to be operated on.
	maybeFlush := func(target ident.Schematic, expect hlc.Time) error {
		cdcTarget, err := h.Conveyors.Get(target.Schema())
		if err != nil {
			return err
		}
		// Wait for minimum timestamp to advance to desired.
		for {
			bounds, boundsChanged := cdcTarget.Range().Get()
			if hlc.Compare(bounds.Min(), expect) >= 0 {
				return nil
			}
			select {
			case <-boundsChanged:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Validate the "classic" endpoints where files containing
	// newline-delimited json and resolved timestamp markers are
	// uploaded.
	//
	// The inner methods of Handler are called directly, to avoid the
	// need to cook up fake http.Request instances.
	t.Run("bulkStyleEndpoints", func(t *testing.T) {
		a := assert.New(t)

		// Stage two lines of data. When this test is run with scripting
		// enabled, we also want to see the bigint value successfully
		// transit the JS runtime without being mangled.
		a.NoError(h.ndjson(ctx, &request{
			target: tableInfo.Name(),
			body: strings.NewReader(`
{"after" : {"pk" : 42, "v" : 9007199254740995}, "updated": "1.0"}
{"after":  {"pk" : 99, "v" : 42}, "updated": "1.0", "before": {"pk" : 99, "v" : 33 }}
`),
		}, h.parseNdjsonQueryMutation))

		// Verify staged data, if applicable.
		if !htc.immediate {
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(),
				hlc.RangeIncluding(hlc.Zero(), hlc.New(1, 1)))
			if a.NoError(err) {
				a.Len(muts, 2)
				// The order is stable since the underlying query
				// is ordered, in part, by the key.
				a.Equal([]types.Mutation{
					{
						Data: []byte(`{"pk":42,"v":9007199254740995}`),
						Key:  []byte(`[42]`),
						Time: hlc.New(1, 0),
					},
					{
						Before: []byte(`{"pk":99,"v":33}`),
						Data:   []byte(`{"pk":99,"v":42}`),
						Key:    []byte(`[99]`),
						Time:   hlc.New(1, 0),
					},
				}, muts)
			}
		}

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
{"before":{"pk" : 42, "v" : null},"updated": "3.0"}
{"before":{"pk" : 99, "v" : null},"updated": "3.0"}
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
		// Insert data and verify flushing.
		a.NoError(h.webhookForQuery(ctx, &request{
			target: tableInfo.Name(),
			body: strings.NewReader(`
{ "payload" : [
	{"after": {"pk" : 42, "v" : 9007199254740995}, "updated": "10.0"},
	{"after": {"pk" : 99, "v" : 42}, "before": { "pk" : 99, "v" : 21 }, "updated": "10.0"}
] }
`),
		}))

		// Verify staged and un-applied data, if applicable.
		if !htc.immediate {
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(),
				hlc.RangeIncluding(hlc.Zero(), hlc.New(10, 1)))
			if a.NoError(err) {
				a.Len(muts, 2)
				// The order is stable since the underlying query
				// orders by HLC and key.
				a.Equal([]types.Mutation{
					{
						Data: []byte(`{"pk":42,"v":9007199254740995}`),
						Key:  []byte(`[42]`),
						Time: hlc.New(10, 0),
					},
					{
						Before: []byte(`{"pk":99,"v":21}`),
						Data:   []byte(`{"pk":99,"v":42}`),
						Key:    []byte(`[99]`),
						Time:   hlc.New(10, 0),
					},
				}, muts)
			}
		}

		a.NoError(h.webhookForQuery(ctx, &request{
			target: tableInfo.Name(),
			body:   strings.NewReader(`{ "resolved" : "20.0" }`),
		}))
		a.NoError(maybeFlush(tableInfo.Name().Schema(), hlc.New(20, 0)))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)

		// Now, delete the data.
		a.NoError(h.webhookForQuery(ctx, &request{
			target: tableInfo.Name(),
			body: strings.NewReader(`
{ "payload" : [
	{"before": {"pk" : 42, "v" : null}, "updated": "30.0"},
	{"before": { "pk" : 99, "v" : null}, "updated": "30.0"}
] }
`),
		}))

		a.NoError(h.webhookForQuery(ctx, &request{
			target: tableInfo.Name(),
			body:   strings.NewReader(`{"resolved" : "40.0" }`),
		}))
		a.NoError(maybeFlush(tableInfo.Name().Schema(), hlc.New(40, 0)))

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
		a.NoError(h.webhookForQuery(ctx, &request{
			target: tableInfo.Name(),
			body:   strings.NewReader(""),
		}))
	})
}

func testHandler(t *testing.T, cfg *fixtureConfig) {
	t.Parallel()
	t.Helper()
	fixture, tableInfo := createFixture(t, cfg)
	ctx := fixture.Context
	h := fixture.Handler

	// Ensure that we can handle identifiers that don't align with
	// what's actually in the target database.
	jumbleName := func() ident.Table {
		return sinktest.JumbleTable(tableInfo.Name())
	}

	// In async mode, we want to reach into the implementation to
	// force the marked, resolved timestamp to be operated on.
	maybeFlush := func(target ident.Schematic, expect hlc.Time) error {
		cdcTarget, err := h.Conveyors.Get(target.Schema())
		if err != nil {
			return err
		}
		cdcTarget.Refresh()
		// Wait for minimum timestamp to advance to desired.
		for {
			bounds, boundsChanged := cdcTarget.Range().Get()
			if hlc.Compare(bounds.Min(), expect) >= 0 {
				return nil
			}
			select {
			case <-boundsChanged:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
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
			target: jumbleName(),
			body: strings.NewReader(`
{ "after" : { "pk" : 42, "v" : 99 }, "key" : [ 42 ], "updated" : "1.0" }
{ "after" : { "pk" : 99, "v" : 42 }, "before": { "pk" : 99, "v" : 33 }, "key" : [ 99 ], "updated" : "1.0" }
`),
		}, parseNdjsonMutation))

		// Verify staged data, if applicable.
		if !cfg.immediate {
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(),
				hlc.RangeIncluding(hlc.Zero(), hlc.New(1, 1)))
			if a.NoError(err) {
				a.Len(muts, 2)
				// The order is stable since the underlying query
				// orders, in part, on key
				a.Equal([]types.Mutation{
					{
						Data: []byte(`{ "pk" : 42, "v" : 99 }`),
						Key:  []byte(`[ 42 ]`),
						Time: hlc.New(1, 0),
					},
					{
						Before: []byte(`{ "pk" : 99, "v" : 33 }`),
						Data:   []byte(`{ "pk" : 99, "v" : 42 }`),
						Key:    []byte(`[ 99 ]`),
						Time:   hlc.New(1, 0),
					},
				}, muts)
			}
		}

		// Send the resolved timestamp request.
		a.NoError(h.resolved(ctx, &request{
			target:    jumbleName(),
			timestamp: hlc.New(2, 0),
		}))
		a.NoError(maybeFlush(jumbleName(), hlc.New(2, 0)))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)

		// Now, delete the data.
		a.NoError(h.ndjson(ctx, &request{
			target: jumbleName(),
			body: strings.NewReader(`
{ "after" : null, "key" : [ 42 ], "updated" : "3.0" }
{ "key" : [ 99 ], "updated" : "3.0" }
`),
		}, parseNdjsonMutation))

		a.NoError(h.resolved(ctx, &request{
			target:    jumbleName(),
			timestamp: hlc.New(5, 0),
		}))
		a.NoError(maybeFlush(jumbleName(), hlc.New(5, 0)))

		ct, err = tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(0, ct)
	})

	// This test validates the new webhook-https:// scheme introduced
	// in CockroachDB v21.2. We insert rows and delete them.
	t.Run("webhookEndpoints", func(t *testing.T) {
		a := assert.New(t)

		// Insert data and verify flushing.
		a.NoError(h.webhook(ctx, &request{
			target: jumbleName().Schema(),
			body: strings.NewReader(fmt.Sprintf(`
{ "payload" : [
  { "after" : { "pk" : 42, "v" : 99 }, "key" : [ 42 ], "topic" : %[1]s, "updated" : "10.0" },
  { "after" : { "pk" : 99, "v" : 42 }, "before": { "pk" : 99, "v" : 21 }, "key" : [ 99 ], "topic" : %[1]s, "updated" : "10.0" }
] }
`, jumbleName().Table())),
		}))

		// Verify staged and as-yet-unapplied data.
		if !cfg.immediate {
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(),
				hlc.RangeIncluding(hlc.Zero(), hlc.New(10, 1)))
			if a.NoError(err) {
				// The order is stable since the underlying query
				// orders by HLC and key.
				a.Equal([]types.Mutation{
					{
						Data: []byte(`{ "pk" : 42, "v" : 99 }`),
						Key:  []byte(`[ 42 ]`),
						Time: hlc.New(10, 0),
					},
					{
						Before: []byte(`{ "pk" : 99, "v" : 21 }`),
						Data:   []byte(`{ "pk" : 99, "v" : 42 }`),
						Key:    []byte(`[ 99 ]`),
						Time:   hlc.New(10, 0),
					},
				}, muts)
			}
		}

		a.NoError(h.webhook(ctx, &request{
			target: jumbleName().Schema(),
			body:   strings.NewReader(`{ "resolved" : "20.0" }`),
		}))
		a.NoError(maybeFlush(jumbleName().Schema(), hlc.New(20, 0)))

		ct, err := tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(2, ct)

		// Now, delete the data.
		a.NoError(h.webhook(ctx, &request{
			target: jumbleName().Schema(),
			body: strings.NewReader(fmt.Sprintf(`
{ "payload" : [
  { "key" : [ 42 ], "topic" : %[1]s, "updated" : "30.0" },
  { "after" : null, "key" : [ 99 ], "topic" : %[1]s, "updated" : "30.0" }
] }
`, jumbleName().Table())),
		}))

		a.NoError(h.webhook(ctx, &request{
			target: jumbleName().Schema(),
			body:   strings.NewReader(`{ "resolved" : "40.0" }`),
		}))
		a.NoError(maybeFlush(jumbleName().Schema(), hlc.New(40, 0)))

		ct, err = tableInfo.RowCount(ctx)
		a.NoError(err)
		a.Equal(0, ct)
	})

	// Verify that an empty post doesn't crash.
	t.Run("empty-ndjson", func(t *testing.T) {
		a := assert.New(t)
		a.NoError(h.ndjson(ctx, &request{
			target: jumbleName(),
			body:   strings.NewReader(""),
		}, parseNdjsonMutation))
	})

	// Verify that an empty webhook post doesn't crash.
	t.Run("empty-webhook", func(t *testing.T) {
		a := assert.New(t)
		a.NoError(h.webhook(ctx, &request{
			target: jumbleName().Schema(),
			body:   strings.NewReader(""),
		}))
	})
}

func TestDiscard(t *testing.T) {
	a := assert.New(t)
	fixture, _ := createFixture(t, &fixtureConfig{discard: true})
	h := fixture.Handler

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/anything", strings.NewReader("Ignored world"))
	h.ServeHTTP(rec, req)

	a.Equal(200, rec.Code)
}

// TestMergeInt ensures that we have a clean, end-to-end test of a
// user apply+merge setup that uses integer values.
func TestMergeInt(t *testing.T) {
	r := require.New(t)
	fixture, _ := createFixture(t, &fixtureConfig{
		immediate: true,
	})

	if !apply.IsMergeSupported(fixture.TargetPool.Product) {
		t.Skipf("merge not implemented for %s", fixture.TargetPool.Product)
	}

	ctx := fixture.Context
	h := fixture.Handler

	table, err := fixture.CreateTargetTable(ctx, `
CREATE TABLE %s (
pk INT8 PRIMARY KEY,
version INT8 NOT NULL,
v INT8 NOT NULL)`)
	r.NoError(err)

	// Configure the table with a basic merge setup.
	cfg := applycfg.NewConfig().Patch(&applycfg.Config{
		CASColumns: []ident.Ident{ident.New("version")},
		Merger:     &merge.Standard{Fallback: merge.DLQ("dead")},
	})
	r.NoError(fixture.Configs.Set(table.Name(), cfg))

	sendUpdate := func(version int, v int64) error {
		return h.ndjson(ctx, &request{
			target: table.Name(),
			body: strings.NewReader(fmt.Sprintf(`
{ "after" : { "pk" : 42, "version": %d, "v" : %d }, "key" : [ 42 ], "updated" : "1.0" }`,
				version,
				v,
			)),
		}, parseNdjsonMutation)
	}

	// Send an update at V9. We're starting with 9 to ensure that when
	// we increase to 10 we're getting a numeric, not a lexicographical
	// comparison.
	r.NoError(sendUpdate(9, 1<<55))

	var v int64
	r.NoError(fixture.TargetPool.QueryRow(fmt.Sprintf(
		"SELECT v FROM %s", table.Name())).Scan(&v))
	r.Equal(int64(1<<55), v)

	// Send an update at V8 that would normally go to the DLQ, had we
	// created one. This is a sanity-check for the idempotent check
	// below.
	r.ErrorContains(sendUpdate(8, 1), "must be created")

	// Repeat the T0 update to ensure that idempotent updates are OK.
	r.NoError(sendUpdate(9, 1<<55))

	// Send a T+1 update. This is expected to succeed.
	r.NoError(sendUpdate(10, 1<<56))

	r.NoError(fixture.TargetPool.QueryRow(fmt.Sprintf(
		"SELECT v FROM %s", table.Name())).Scan(&v))
	r.Equal(int64(1<<56), v)
}

// TestMergeTime ensures that we have a clean, end-to-end test of a
// user apply+merge setup that uses timestamp values.
func TestMergeTime(t *testing.T) {
	r := require.New(t)
	fixture, _ := createFixture(t, &fixtureConfig{
		immediate: true,
	})

	if !apply.IsMergeSupported(fixture.TargetPool.Product) {
		t.Skipf("merge not implemented for %s", fixture.TargetPool.Product)
	}

	ctx := fixture.Context
	h := fixture.Handler

	table, err := fixture.CreateTargetTable(ctx, `
CREATE TABLE %s (
pk INT8 PRIMARY KEY,
updated_at TIMESTAMP NOT NULL,
v INT8 NOT NULL)`)
	r.NoError(err)

	// Configure the table with a basic merge setup.
	cfg := applycfg.NewConfig().Patch(&applycfg.Config{
		CASColumns: []ident.Ident{ident.New("updated_at")},
		Merger:     &merge.Standard{Fallback: merge.DLQ("dead")},
	})
	r.NoError(fixture.Configs.Set(table.Name(), cfg))

	sendUpdate := func(ts time.Time, v int64) error {
		return h.ndjson(ctx, &request{
			target: table.Name(),
			body: strings.NewReader(fmt.Sprintf(`
{ "after" : { "pk" : 42, "updated_at": %q, "v" : %d }, "key" : [ 42 ], "updated" : "1.0" }`,
				ts.Format(time.RFC3339Nano),
				v,
			)),
		}, parseNdjsonMutation)
	}

	// Round time to reflect actual storage.
	now := time.Now().UTC().Round(time.Microsecond)

	// Send an update at T0.
	r.NoError(sendUpdate(now, 1<<55))

	var v int64
	r.NoError(fixture.TargetPool.QueryRow(fmt.Sprintf(
		"SELECT v FROM %s", table.Name())).Scan(&v))
	r.Equal(int64(1<<55), v)

	// Send an update at T-1 that would normally go to the DLQ, had we
	// created one. This is a sanity-check for the idempotent check
	// below.
	r.ErrorContains(sendUpdate(now.Add(-time.Minute), 1), "must be created")

	// Repeat the T0 update to ensure that idempotent updates are OK.
	r.NoError(sendUpdate(now, 1<<55))

	// Send a T+1 update. This is expected to succeed.
	r.NoError(sendUpdate(now.Add(time.Minute), 1<<56))

	r.NoError(fixture.TargetPool.QueryRow(fmt.Sprintf(
		"SELECT v FROM %s", table.Name())).Scan(&v))
	r.Equal(int64(1<<56), v)
}

func TestRejectedAuth(t *testing.T) {
	// Verify that auth checks don't require other services.
	h := &Handler{
		Authenticator: reject.New(),
		Config:        &Config{},
		TargetPool: &types.TargetPool{
			PoolInfo: types.PoolInfo{
				Product: types.ProductCockroachDB,
			},
		},
	}
	require.NoError(t, h.Config.Preflight())

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
