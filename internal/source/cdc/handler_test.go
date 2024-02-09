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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/scripttest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/auth/reject"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type fixtureConfig struct {
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
	// function.
	var schema string
	if htc.script {
		schema = `CREATE TABLE %s (pk INT PRIMARY KEY, v_mapped INT NOT NULL)`
	} else {
		schema = `CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`
	}

	ctx := baseFixture.Context
	tableInfo, err := baseFixture.CreateTargetTable(ctx, schema)
	r.NoError(err)

	cfg := &Config{
		BackfillWindow: math.MaxInt64,
		Immediate:      htc.immediate,
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
		if htc.immediate {
			return nil
		}
		tgt, err := h.Targets.getTarget(target.Schema())
		if err != nil {
			return err
		}
		// Wait for minimum timestamp to advance to desired.
		for {
			bounds, boundsChanged := tgt.resolvingRange.Get()
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
			target: tableInfo.Name(),
			body: strings.NewReader(`
{"__event__": "insert", "pk" : 42, "v" : 99, "__crdb__": {"updated": "1.0"}}
{"__event__": "insert", "pk" : 99, "v" : 42, "__crdb__": {"updated": "1.0"}, "cdc_prev": {"pk" : 99, "v" : 33 }}
`),
		}, h.parseNdjsonQueryMutation))

		// Verify staged data, if applicable.
		if !htc.immediate {
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(), hlc.Zero(), hlc.New(1, 1))
			if a.NoError(err) {
				a.Len(muts, 2)
				// The order is stable since the underlying query
				// is ordered, in part, by the key.
				a.Equal([]types.Mutation{
					{
						Data: []byte(`{"pk":42,"v":99}`),
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
		// Insert data and verify flushing.
		a.NoError(h.webhookForQuery(ctx, &request{
			target: tableInfo.Name(),
			body: strings.NewReader(`
{ "payload" : [
	{"__event__": "insert", "pk" : 42, "v" : 99, "__crdb__": {"updated": "10.0"}},
	{"__event__": "insert", "pk" : 99, "v" : 42, "cdc_prev": { "pk" : 99, "v" : 21 }, "__crdb__": {"updated": "10.0"}}
] }
`),
		}))

		// Verify staged and un-applied data, if applicable.
		if !htc.immediate {
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(), hlc.Zero(), hlc.New(10, 1))
			if a.NoError(err) {
				a.Len(muts, 2)
				// The order is stable since the underlying query
				// orders by HLC and key.
				a.Equal([]types.Mutation{
					{
						Data: []byte(`{"pk":42,"v":99}`),
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
			body:   strings.NewReader(`{ "__crdb__": {"resolved" : "20.0" }}`),
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
	{"__event__": "delete", "pk" : 42, "v" : null, "__crdb__": {"updated": "30.0"}},
	{"__event__": "delete", "pk" : 99, "v" : null, "__crdb__": {"updated": "30.0"}}
] }
`),
		}))

		a.NoError(h.webhookForQuery(ctx, &request{
			target: tableInfo.Name(),
			body:   strings.NewReader(`{ "__crdb__": {"resolved" : "40.0" }}`),
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
		if cfg.immediate {
			return nil
		}
		tgt, err := h.Targets.getTarget(target.Schema())
		if err != nil {
			return err
		}
		tgt.checkpoint.Refresh()
		// Wait for minimum timestamp to advance to desired.
		for {
			bounds, boundsChanged := tgt.resolvingRange.Get()
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
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(), hlc.Zero(), hlc.New(1, 1))
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
			muts, err := fixture.PeekStaged(ctx, tableInfo.Name(), hlc.Zero(), hlc.New(10, 1))
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

func TestRejectedAuth(t *testing.T) {
	// Verify that auth checks don't require other services.
	h := &Handler{
		Authenticator: reject.New(),
		TargetPool: &types.TargetPool{
			PoolInfo: types.PoolInfo{
				Product: types.ProductCockroachDB,
			},
		},
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
	const handlers = 1
	const rowCount = 10_000
	t.Run("normal-besteffort", func(t *testing.T) {
		testMassBackfillWithForeignKeys(t, rowCount, handlers, func(cfg *Config) {
			cfg.BackfillWindow = time.Minute
		})
	})
	t.Run("normal-serial", func(t *testing.T) {
		testMassBackfillWithForeignKeys(t, rowCount, handlers)
	})
	t.Run("normal-shingle", func(t *testing.T) {
		testMassBackfillWithForeignKeys(t, rowCount, handlers, func(cfg *Config) {
			cfg.Shingle = true
		})
	})
	t.Run("normal-transactional-flush-every", func(t *testing.T) {
		// Reduced row count since throughput is compromised.
		testMassBackfillWithForeignKeys(t, rowCount/10, handlers, func(cfg *Config) {
			cfg.SequencerConfig.TimestampLimit = 1
		})
	})
}

// This test simulates having multiple instances of the handler as we
// would see in a load-balanced configuration.
func TestConcurrentHandlers(t *testing.T) {
	const handlers = 3
	const rowCount = 10_000

	t.Run("normal-besteffort", func(t *testing.T) {
		testMassBackfillWithForeignKeys(t, rowCount, handlers, func(cfg *Config) {
			cfg.BackfillWindow = time.Minute
		})
	})
	t.Run("normal-serial", func(t *testing.T) {
		testMassBackfillWithForeignKeys(t, rowCount, handlers)
	})
	t.Run("normal-shingle", func(t *testing.T) {
		testMassBackfillWithForeignKeys(t, rowCount, handlers, func(cfg *Config) {
			cfg.Shingle = true
		})
	})
	t.Run("normal-transactional-flush-every", func(t *testing.T) {
		// Reduced row count since throughput is compromised.
		testMassBackfillWithForeignKeys(t, rowCount/10, handlers, func(cfg *Config) {
			cfg.SequencerConfig.TimestampLimit = 1
		})
	})
}

func testMassBackfillWithForeignKeys(
	t *testing.T, rowCount, fixtureCount int, fns ...func(*Config),
) {
	t.Parallel()
	r := require.New(t)

	baseFixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := baseFixture.Context

	fixtures := make([]*testFixture, fixtureCount)

	for idx := range fixtures {
		cfg := &Config{
			SequencerConfig: sequencer.Config{
				Parallelism:     2,
				QuiescentPeriod: time.Second,
				RetireOffset:    time.Hour,
				SweepLimit:      rowCount / 3, // Read the same timestamp more than once.
			},
		}
		for _, fn := range fns {
			fn(cfg)
		}
		fixtures[idx], err = newTestFixture(baseFixture, cfg)
		r.NoError(err)
	}

	loadStart := time.Now()
	log.Info("starting data load")

	parent, err := baseFixture.CreateTargetTable(ctx,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	r.NoError(err)

	child, err := baseFixture.CreateTargetTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (pk INT PRIMARY KEY, FOREIGN KEY(pk) REFERENCES %s(pk), v INT NOT NULL)`,
		parent.Name()))
	r.NoError(err)

	grand, err := baseFixture.CreateTargetTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (pk INT PRIMARY KEY, FOREIGN KEY(pk) REFERENCES %s(pk), v INT NOT NULL)`,
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
		timestamp: hlc.New(int64(rowCount)+1, 0),
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

	// Ensure that there are no un-applied rows in the staging table.
	// Because the staging tables may be a separate database from the
	// target, there's a race condition between waiting for rows to
	// appear above and waiting for the staging rows to be marked as
	// applied.
	for _, tbl := range []interface{ Name() ident.Table }{parent, child, grand} {
		for {
			found, err := fixtures[0].PeekStaged(ctx, tbl.Name(),
				hlc.Zero(), hlc.New(int64(rowCount)+1, 0))
			r.NoError(err)
			log.Infof("saw %d unapplied staging entries in %s", len(found), tbl)
			if len(found) == 0 {
				break
			}
			time.Sleep(time.Second)
		}
	}
}
