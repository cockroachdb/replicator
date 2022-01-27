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
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/gofrs/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// This test inserts and deletes rows from a trivial table.
func TestApply(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
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

	app, cancel, err := newApply(watcher, tbl.Name(), nil /* casColumns */, types.Deadlines{})
	if !a.NoError(err) {
		return
	}
	defer cancel()

	log.Debug(app.mu.sql.delete)
	log.Debug(app.mu.sql.upsert)

	t.Run("smoke", func(t *testing.T) {
		a := assert.New(t)
		count := 3 * batches.Size()
		adds := make([]types.Mutation, count)
		dels := make([]types.Mutation, count)
		for i := range adds {
			p := Payload{Pk0: i, Pk1: fmt.Sprintf("X%dX", i)}
			bytes, err := json.Marshal(p)
			a.NoError(err)
			adds[i] = types.Mutation{Data: bytes}

			bytes, err = json.Marshal([]interface{}{p.Pk0, p.Pk1})
			a.NoError(err)
			dels[i] = types.Mutation{Key: bytes}
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
		if err := app.Apply(ctx, dbInfo.Pool(), []types.Mutation{
			{
				Data: []byte(`{"pk0":1, "pk1":0, "no_good":true}`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "unexpected columns [no_good]")
		}
	})

	t.Run("missing_key_upsert", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, dbInfo.Pool(), []types.Mutation{
			{
				Data: []byte(`{"pk0":1}`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "missing PK column pk1")
		}
	})

	t.Run("missing_key_delete_too_few", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, dbInfo.Pool(), []types.Mutation{
			{
				Key: []byte(`[1]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "received 1 expect 2")
		}
	})

	t.Run("missing_key_delete_too_many", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, dbInfo.Pool(), []types.Mutation{
			{
				Key: []byte(`[1, 2, 3]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "received 3 expect 2")
		}
	})
}

// This is a smoke test, copied from main_test.go to ensure that
// all supported data types can be applied. It works by creating
// a test table for each type and using CRDB's built-in to_jsonb()
// function to create a payload.
func TestAllDataTypes(t *testing.T) {
	testcases := []struct {
		name        string
		columnType  string
		columnValue string
		indexable   bool
	}{
		{`string_array`, `STRING[]`, `{"sky","road","car"}`, false},
		{`string_array_null`, `STRING[]`, ``, false},
		{`int_array`, `INT[]`, `{1,2,3}`, false},
		{`int_array_null`, `INT[]`, ``, false},
		{`serial_array`, `SERIAL[]`, `{148591304110702593,148591304110702594,148591304110702595}`, false},
		{`serial_array_null`, `SERIAL[]`, ``, false},
		{`bit`, `VARBIT`, `10010101`, true},
		{`bit_null`, `VARBIT`, ``, false},
		{`bool`, `BOOL`, `true`, true},
		{`bool_array`, `BOOL[]`, `{true, false, true}`, false},
		{`bool_null`, `BOOL`, ``, false},
		{`bytes`, `BYTES`, `b'\141\061\142\062\143\063'`, true},
		{`collate`, `STRING COLLATE de`, `'a1b2c3' COLLATE de`, true},
		{`collate_null`, `STRING COLLATE de`, ``, false},
		{`date`, `DATE`, `2016-01-25`, true},
		{`date_null`, `DATE`, ``, false},
		{`decimal`, `DECIMAL`, `1.2345`, true},
		{`decimal_null`, `DECIMAL`, ``, false},
		{`float`, `FLOAT`, `1.2345`, true},
		{`float_null`, `FLOAT`, ``, false},
		{`geography`, `GEOGRAPHY`, `0101000020E6100000000000000000F03F0000000000000040`, false},
		{`geometry`, `GEOMETRY`, `010100000075029A081B9A5DC0F085C954C1F84040`, false},
		{`inet`, `INET`, `192.168.0.1`, true},
		{`inet_null`, `INET`, ``, false},
		{`int`, `INT`, `12345`, true},
		{`int_null`, `INT`, ``, false},
		{`interval`, `INTERVAL`, `2h30m30s`, true},
		{`interval_null`, `INTERVAL`, ``, false},
		{
			`jsonb`,
			`JSONB`,
			`
			{
				"string": "Lola",
				"bool": true,
				"number": 547,
				"float": 123.456,
				"array": [
					"lola",
					true,
					547,
					123.456,
					[
						"lola",
						true,
						547,
						123.456
					],
					{
						"string": "Lola",
						"bool": true,
						"number": 547,
						"float": 123.456,
						"array": [
							"lola",
							true,
							547,
							123.456,
							[
								"lola",
								true,
								547,
								123.456
							]
						]
					}
				],
				"map": {
					"string": "Lola",
					"bool": true,
					"number": 547,
					"float": 123.456,
					"array": [
						"lola",
						true,
						547,
						123.456,
						[
							"lola",
							true,
							547,
							123.456
						],
						{
							"string": "Lola",
							"bool": true,
							"number": 547,
							"float": 123.456,
							"array": [
								"lola",
								true,
								547,
								123.456,
								[
									"lola",
									true,
									547,
									123.456
								]
							]
						}
					]
				}
			}
			`,
			false,
		},
		{`jsonb_null`, `JSONB`, ``, false},
		{`serial`, `SERIAL`, `148591304110702593`, true},
		// serial cannot be null
		{`string`, `STRING`, `a1b2c3`, true},
		{`string_null`, `STRING`, ``, false},
		{`string_escape`, `STRING`, `a1\b/2?c"3`, true},
		{`time`, `TIME`, `01:23:45.123456`, true},
		{`time_null`, `TIME`, ``, false},
		{`timestamp`, `TIMESTAMP`, `2016-01-25 10:10:10`, true},
		{`timestamp_null`, `TIMESTAMP`, ``, false},
		{`timestamptz`, `TIMESTAMPTZ`, `2016-01-25 10:10:10-05:00`, true},
		{`timestamptz_null`, `TIMESTAMPTZ`, ``, false},
		{`uuid`, `UUID`, `7f9c24e8-3b12-4fef-91e0-56a2d5a246ec`, true},
		{`uuid_null`, `UUID`, ``, false},
	}

	a := assert.New(t)

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)

			// Place the PK index on the data type under test, if allowable.
			var create string
			if tc.indexable {
				create = fmt.Sprintf("CREATE TABLE %%s (val %s primary key, k int)", tc.columnType)
			} else {
				create = fmt.Sprintf("CREATE TABLE %%s (k int primary key, val %s)", tc.columnType)
			}

			tbl, err := sinktest.CreateTable(ctx, dbName, create)
			if !a.NoError(err) {
				return
			}

			watcher, err := watchers.Get(ctx, dbName)
			if !a.NoError(err) {
				return
			}

			if !a.NoError(watcher.Refresh(ctx, dbInfo.Pool())) {
				return
			}

			app, cancel, err := newApply(watcher, tbl.Name(), nil /* casColumns */, types.Deadlines{})
			if !a.NoError(err) {
				return
			}
			defer cancel()

			log.Debug(app.mu.sql.delete)
			log.Debug(app.mu.sql.upsert)

			var jsonValue string
			if tc.columnValue == "" {
				jsonValue = "null"
			} else {
				q := fmt.Sprintf("SELECT to_json($1::%s)::string", tc.columnType)
				if !a.NoError(dbInfo.Pool().QueryRow(ctx, q, tc.columnValue).Scan(&jsonValue)) {
					return
				}
			}
			log.Debug(jsonValue)

			mut := types.Mutation{
				Data: []byte(fmt.Sprintf(`{"k":1,"val":%s}`, jsonValue)),
			}
			a.NoError(app.Apply(ctx, dbInfo.Pool(), []types.Mutation{mut}))

			var jsonFound string
			a.NoError(dbInfo.Pool().QueryRow(ctx,
				fmt.Sprintf("SELECT ifnull(to_json(val)::string, 'null') FROM %s", tbl),
			).Scan(&jsonFound))
			a.Equal(jsonValue, jsonFound)
		})
	}
}

// This tests compare-and-set behaviors.
func TestConditionals(t *testing.T) {
	t.Run("base", func(t *testing.T) { testConditions(t, false, false) })
	t.Run("cas", func(t *testing.T) { testConditions(t, true, false) })
	t.Run("deadline", func(t *testing.T) { testConditions(t, false, true) })
	t.Run("cas+deadline", func(t *testing.T) { testConditions(t, true, true) })
}

func testConditions(t *testing.T, cas, deadline bool) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// The payload and the table are just a key and version field.
	type Payload struct {
		PK  int       `json:"pk"`
		Ver int       `json:"ver"`
		TS  time.Time `json:"ts"`
	}

	tbl, err := sinktest.CreateTable(ctx, dbName,
		"CREATE TABLE %s (pk INT PRIMARY KEY, ver INT, ts TIMESTAMP)")
	if !a.NoError(err) {
		return
	}

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()
	watcher, err := watchers.Get(ctx, dbName)
	if !a.NoError(err) {
		return
	}

	t.Run("check_invalid_cas_name", func(t *testing.T) {
		a := assert.New(t)
		_, cancel, err := newApply(watcher, tbl.Name(), []ident.Ident{ident.New("bad_column")}, types.Deadlines{})
		defer cancel()
		if a.Error(err) {
			a.Contains(err.Error(), "bad_column")
		}
	})

	t.Run("check_invalid_deadline_name", func(t *testing.T) {
		a := assert.New(t)
		_, cancel, err := newApply(watcher, tbl.Name(), nil, types.Deadlines{ident.New("bad_column"): 0})
		defer cancel()
		if a.Error(err) {
			a.Contains(err.Error(), "bad_column")
		}
	})

	// The PK value is a fixed value.
	const id = 42

	// Utility function to retrieve the most recently set data.
	getRow := func() (version int, ts time.Time, err error) {
		err = dbInfo.Pool().QueryRow(ctx,
			fmt.Sprintf("SELECT ver, ts FROM %s WHERE pk = $1", tbl.Name()), id,
		).Scan(&version, &ts)
		return
	}

	// Set up the apply instance, per the configuration.
	var casColumns []ident.Ident
	if cas {
		casColumns = []ident.Ident{ident.New("ver")}
	}
	deadlines := types.Deadlines{}
	if deadline {
		deadlines[ident.New("ts")] = 10 * time.Minute
	}
	app, cancel, err := newApply(watcher, tbl.Name(), casColumns, deadlines)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	log.Debug(app.mu.sql.delete)
	log.Debug(app.mu.sql.upsert)

	now := time.Now().UTC().Round(time.Second)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	proposals := []struct {
		version int
		ts      time.Time
	}{
		{0, future}, // Ensure that a zero-version can be inserted.
		{100, future},
		{5, future},
		{200, past},
		{9, past},
		{300, future},
	}
	var lastTime time.Time
	lastVersion := -1

	for _, proposed := range proposals {
		var shouldApply bool
		switch {
		case !cas && !deadline:
			shouldApply = true
		case cas && deadline:
			shouldApply = proposed.version > lastVersion && proposed.ts == future
		case cas:
			shouldApply = proposed.version > lastVersion
		case deadline:
			shouldApply = proposed.ts == future
		}

		expectedTime := lastTime
		expectedVersion := lastVersion
		if shouldApply {
			expectedTime = proposed.ts
			expectedVersion = proposed.version
		}

		p := Payload{PK: id, Ver: proposed.version, TS: proposed.ts}
		bytes, err := json.Marshal(p)
		a.NoError(err)

		// Applying a discarded mutation should never result in an error.
		a.NoError(app.Apply(ctx, dbInfo.Pool(), []types.Mutation{{Data: bytes}}))

		ct, err := tbl.RowCount(ctx)
		a.Equal(1, ct)
		a.NoError(err)

		// Test the version that's currently in the database, make sure
		// that we haven't gone backwards.
		ver, ts, err := getRow()
		a.Equal(expectedTime, ts)
		a.Equal(expectedVersion, ver)
		a.NoError(err)

		lastTime = expectedTime
		lastVersion = expectedVersion
	}
}

// Ensure that if stored computed columns are present, we don't
// try to write to them and that we correctly ignore those columns
// in incoming payloads.
func TestVirtualColumns(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	type Payload struct {
		A int `json:"a"`
		B int `json:"b"`
		C int `json:"c"`
		X int `json:"x,omitempty"`
	}
	tbl, err := sinktest.CreateTable(ctx, dbName,
		"CREATE TABLE %s (a INT, b INT, c INT AS (a + b) STORED, PRIMARY KEY (a,b))")
	if !a.NoError(err) {
		return
	}

	watcher, err := watchers.Get(ctx, dbName)
	if !a.NoError(err) {
		return
	}

	app, cancel, err := newApply(watcher, tbl.Name(), nil /* casColumns */, types.Deadlines{})
	if !a.NoError(err) {
		return
	}
	defer cancel()

	log.Debug(app.mu.sql.delete)
	log.Debug(app.mu.sql.upsert)

	t.Run("computed-is-ignored", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		muts := []types.Mutation{{Data: bytes}}

		a.NoError(app.Apply(ctx, dbInfo.Pool(), muts))
	})

	t.Run("unknown-still-breaks", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, X: -1}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		muts := []types.Mutation{{Data: bytes}}

		err = app.Apply(ctx, dbInfo.Pool(), muts)
		if a.Error(err) {
			a.Contains(err.Error(), "unexpected columns")
		}
	})
}

func BenchmarkApply(b *testing.B) {
	b.Run("base", func(b *testing.B) { benchConditions(b, false, false) })
	b.Run("cas", func(b *testing.B) { benchConditions(b, true, false) })
	b.Run("deadline", func(b *testing.B) { benchConditions(b, false, true) })
	b.Run("cas+deadline", func(b *testing.B) { benchConditions(b, true, true) })
}

func benchConditions(b *testing.B, cas, deadline bool) {
	a := assert.New(b)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	tbl, err := sinktest.CreateTable(ctx, dbName,
		"CREATE TABLE %s (pk UUID PRIMARY KEY, ver INT, ts TIMESTAMP)")
	if !a.NoError(err) {
		return
	}

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()
	watcher, err := watchers.Get(ctx, dbName)
	if !a.NoError(err) {
		return
	}

	// Set up the apply instance, per the configuration.
	var casColumns []ident.Ident
	if cas {
		casColumns = []ident.Ident{ident.New("ver")}
	}
	deadlines := types.Deadlines{}
	if deadline {
		deadlines[ident.New("ts")] = time.Hour
	}
	app, cancel, err := newApply(watcher, tbl.Name(), casColumns, deadlines)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// The payload and the table are just a key and version field.
	type Payload struct {
		PK  uuid.UUID `json:"pk"`
		Ver int       `json:"ver"`
		TS  time.Time `json:"ts"`
	}

	// Initialize a finite number of records. Each worker will increment
	// the version counter, so the CAS operations will perform an
	// update.
	now := time.Now().UTC().Round(time.Second)
	const rowCount = 10000
	var data [rowCount]Payload
	for i := range data {
		data[i].PK, _ = uuid.NewV4()
		data[i].TS = now
	}

	b.ResetTimer()
	var sharedIndex int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&sharedIndex, 1) % rowCount
			data[idx].Ver++
			bytes, err := json.Marshal(data[idx])
			if !a.NoError(err) {
				return
			}

			// Applying a discarded mutation should never result in an error.
			if !a.NoError(app.Apply(context.Background(), dbInfo.Pool(), []types.Mutation{{Data: bytes}})) {
				return
			}
		}
	})
}
