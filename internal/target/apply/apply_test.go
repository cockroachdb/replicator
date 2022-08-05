// Copyright 2021 The Cockroach Authors.
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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test inserts and deletes rows from a trivial table.
func TestApply(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	type Payload struct {
		Pk0 int    `json:"pk0"`
		Pk1 string `json:"pk1"`
	}
	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk0 INT, pk1 STRING, extras JSONB, PRIMARY KEY (pk0,pk1))")
	if !a.NoError(err) {
		return
	}

	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

	t.Run("smoke", func(t *testing.T) {
		a := assert.New(t)
		count := 3 * batches.Size()
		adds := make([]types.Mutation, count)
		dels := make([]types.Mutation, count)
		for i := range adds {
			p := Payload{Pk0: i, Pk1: fmt.Sprintf("X%dX", i)}
			bytes, err := json.Marshal(p)
			a.NoError(err)
			adds[i] = types.Mutation{Data: bytes, Key: bytes}

			bytes, err = json.Marshal([]interface{}{p.Pk0, p.Pk1})
			a.NoError(err)
			dels[i] = types.Mutation{Key: bytes}
		}

		// Verify insertion
		a.NoError(app.Apply(ctx, fixture.Pool, adds))
		ct, err := tbl.RowCount(ctx)
		a.Equal(count, ct)
		a.NoError(err)

		// Verify that they can be deleted.
		a.NoError(app.Apply(ctx, fixture.Pool, dels))
		ct, err = tbl.RowCount(ctx)
		a.Equal(0, ct)
		a.NoError(err)
	})

	// Verify unexpected incoming column
	t.Run("unexpected", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, fixture.Pool, []types.Mutation{
			{
				Data: []byte(`{"pk0":1, "pk1":0, "no_good":true}`),
				Key:  []byte(`[1, 0]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "unexpected columns [no_good]")
		}
	})

	t.Run("missing_key_upsert", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, fixture.Pool, []types.Mutation{
			{
				Data: []byte(`{"pk0":1}`),
				Key:  []byte(`[1]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "missing PK column pk1")
		}
	})

	t.Run("missing_key_delete_too_few", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, fixture.Pool, []types.Mutation{
			{
				Key: []byte(`[1]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "received 1 expect 2")
		}
	})

	t.Run("missing_key_delete_too_many", func(t *testing.T) {
		a := assert.New(t)
		if err := app.Apply(ctx, fixture.Pool, []types.Mutation{
			{
				Key: []byte(`[1, 2, 3]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "received 3 expect 2")
		}
	})

	// Verify that unknown columns can be saved.
	t.Run("extras", func(t *testing.T) {
		a := assert.New(t)
		cfg := apply.NewConfig()
		cfg.Extras = ident.New("extras")
		a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), cfg))
		changed, err := fixture.Configs.Refresh(ctx)
		a.True(changed)
		a.NoError(err)

		// The config update is async, so we may need to try again.
		for {
			err := app.Apply(ctx, fixture.Pool, []types.Mutation{
				{
					Data: []byte(`{"pk0":1, "pk1":0, "heretofore":"unseen", "are":"OK"}`),
					Key:  []byte(`[1, 0]`),
				},
				{
					Data: []byte(`{"pk0":2, "pk1":0, "check":"multiple", "mutations":"work"}`),
					Key:  []byte(`[2, 0]`),
				},
			})
			if err == nil {
				break
			}
			if strings.Contains(err.Error(), "unexpected columns") {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			a.NoError(err)
			return
		}

		var extras map[string]string
		a.NoError(fixture.Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT extras FROM %s WHERE pk0=$1 AND pk1=$2", tbl.Name()),
			1, "0",
		).Scan(&extras))
		a.Len(extras, 2)
		a.Equal("unseen", extras["heretofore"])
		a.Equal("OK", extras["are"])

		extras = nil
		a.NoError(fixture.Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT extras FROM %s WHERE pk0=$1 AND pk1=$2", tbl.Name()),
			2, "0",
		).Scan(&extras))
		a.Len(extras, 2)
		a.Equal("multiple", extras["check"])
		a.Equal("work", extras["mutations"])

		a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), nil))
		changed, err = fixture.Configs.Refresh(ctx)
		a.True(changed)
		a.NoError(err)
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
		{`decimal_eng_6,0`, `DECIMAL(6,0)`, `4e+2`, true},
		{`decimal_eng_6,2`, `DECIMAL(6,2)`, `4.98765e+2`, true},
		{`decimal_eng_50,2`, `DECIMAL(600,2)`, `4e+50`, true}, // Bigger than int64
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

	expectInstead := map[string]string{
		"decimal_eng_6,0":  "400",
		"decimal_eng_6,2":  "498.77",
		"decimal_eng_50,2": "400000000000000000000000000000000000000000000000000.00",
	}

	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

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

			tbl, err := fixture.CreateTable(ctx, create)
			if !a.NoError(err) {
				return
			}

			app, err := fixture.Appliers.Get(ctx, tbl.Name())
			if !a.NoError(err) {
				return
			}

			var jsonValue string
			if tc.columnValue == "" {
				jsonValue = "null"
			} else {
				q := fmt.Sprintf("SELECT to_json($1::%s)::string", tc.columnType)
				if !a.NoError(fixture.Pool.QueryRow(ctx, q, tc.columnValue).Scan(&jsonValue)) {
					return
				}
			}
			log.Debug(jsonValue)

			mut := types.Mutation{
				Data: []byte(fmt.Sprintf(`{"k":1,"val":%s}`, jsonValue)),
				Key:  []byte(`[1]`),
			}
			a.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{mut}))

			var jsonFound string
			a.NoError(fixture.Pool.QueryRow(ctx,
				fmt.Sprintf("SELECT ifnull(to_json(val)::string, 'null') FROM %s", tbl),
			).Scan(&jsonFound))
			if alternate, ok := expectInstead[tc.name]; ok {
				a.Equal(alternate, jsonFound)
			} else {
				a.Equal(jsonValue, jsonFound)
			}
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

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	// The payload and the table are just a key and version field.
	type Payload struct {
		PK  int       `json:"pk"`
		Ver int       `json:"ver"`
		TS  time.Time `json:"ts"`
	}

	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk INT PRIMARY KEY, ver INT, ts TIMESTAMP)")
	if !a.NoError(err) {
		return
	}

	t.Run("check_invalid_cas_name", func(t *testing.T) {
		a := assert.New(t)

		a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), &apply.Config{
			CASColumns: []ident.Ident{ident.New("bad_column")},
		}))
		changed, err := fixture.Configs.Refresh(ctx)
		a.True(changed)
		a.NoError(err)

		_, err = fixture.Appliers.Get(ctx, tbl.Name())
		if a.Error(err) {
			a.Contains(err.Error(), "bad_column")
		}
	})

	t.Run("check_invalid_deadline_name", func(t *testing.T) {
		a := assert.New(t)

		a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), &apply.Config{
			Deadlines: types.Deadlines{ident.New("bad_column"): time.Second},
		}))
		changed, err := fixture.Configs.Refresh(ctx)
		a.True(changed)
		a.NoError(err)

		_, err = fixture.Appliers.Get(ctx, tbl.Name())
		if a.Error(err) {
			a.Contains(err.Error(), "bad_column")
		}
	})

	// The PK value is a fixed value.
	const id = 42

	// Utility function to retrieve the most recently set data.
	getRow := func() (version int, ts time.Time, err error) {
		err = fixture.Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT ver, ts FROM %s WHERE pk = $1", tbl.Name()), id,
		).Scan(&version, &ts)
		return
	}

	// Set up the apply instance, per the configuration.
	configData := apply.NewConfig()
	if cas {
		configData.CASColumns = []ident.Ident{ident.New("ver")}
	}
	if deadline {
		configData.Deadlines[ident.New("ts")] = 10 * time.Minute
	}
	a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), configData))
	changed, err := fixture.Configs.Refresh(ctx)
	a.True(changed)
	a.NoError(err)
	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

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
		a.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d]`, id)),
		}}))

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

// Verifies "constant" and substitution params, including cases where
// the PK is subject to rewriting.
func TestExpressionColumns(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK    int    `json:"pk"`
		Val   string `json:"val"`
		Fixed string `json:"fixed"`
	}

	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk INT PRIMARY KEY, val STRING, fixed STRING)")
	if !a.NoError(err) {
		return
	}

	configData := apply.NewConfig()
	configData.Exprs = map[apply.TargetColumn]string{
		ident.New("pk"):    "2 * $0",
		ident.New("val"):   "$0 || ' world!'",
		ident.New("fixed"): "'constant'",
	}
	a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), configData))
	changed, err := fixture.Configs.Refresh(ctx)
	a.True(changed)
	a.NoError(err)
	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

	p := Payload{PK: 42, Val: "Hello", Fixed: "ignored"}
	bytes, err := json.Marshal(p)
	a.NoError(err)

	a.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{{
		Data: bytes,
		Key:  []byte(fmt.Sprintf(`[%d]`, p.PK)),
	}}))

	count, err := sinktest.GetRowCount(ctx, fixture.Pool, tbl.Name())
	a.NoError(err)
	a.Equal(1, count)

	var key int
	var val string
	var fixed string
	a.NoError(fixture.Pool.QueryRow(ctx,
		fmt.Sprintf("SELECT * from %s", tbl)).Scan(&key, &val, &fixed))
	a.Equal(84, key)
	a.Equal("Hello world!", val)
	a.Equal("constant", fixed)

	// Verify that deletes with expressions work.
	a.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{{
		Key: []byte(fmt.Sprintf(`[%d]`, p.PK)),
	}}))
	count, err = sinktest.GetRowCount(ctx, fixture.Pool, tbl.Name())
	a.Equal(0, count)
	a.NoError(err)
}

// This tests ignoring a primary key column, an extant db column,
// and a column which only exists in the incoming payload.
func TestIgnoredColumns(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK0        int    `json:"pk0"`
		PKDeleted  int    `json:"pk_deleted"`
		PK1        int    `json:"pk1"`
		Val0       string `json:"val0"`
		ValIgnored string `json:"val_ignored"`
	}

	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk0 INT, pk1 INT, val0 STRING, not_required STRING, PRIMARY KEY (pk0, pk1))")
	if !a.NoError(err) {
		return
	}

	configData := apply.NewConfig()
	configData.Ignore = map[apply.TargetColumn]bool{
		ident.New("pk_deleted"):   true,
		ident.New("val_ignored"):  true,
		ident.New("not_required"): true,
	}
	a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), configData))
	changed, err := fixture.Configs.Refresh(ctx)
	a.True(changed)
	a.NoError(err)
	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

	p := Payload{PK0: 42, PKDeleted: -1, PK1: 43, Val0: "Hello world!", ValIgnored: "Ignored"}
	bytes, err := json.Marshal(p)
	a.NoError(err)

	a.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{{
		Data: bytes,
		Key:  []byte(fmt.Sprintf(`[%d, %d]`, p.PK0, p.PK1)),
	}}))

	// Verify deletion.
	a.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{{
		Key: []byte(fmt.Sprintf(`[%d, %d]`, p.PK0, p.PK1)),
	}}))
}

// This tests the renaming configuration feature.
func TestRenamedColumns(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK  int    `json:"pk_source"`
		Val string `json:"val_source"`
	}

	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	if !a.NoError(err) {
		return
	}

	configData := apply.NewConfig()
	configData.SourceNames = map[apply.TargetColumn]apply.SourceColumn{
		ident.New("pk"):  ident.New("pk_source"),
		ident.New("val"): ident.New("val_source"),
	}
	a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), configData))
	changed, err := fixture.Configs.Refresh(ctx)
	a.True(changed)
	a.NoError(err)
	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

	p := Payload{PK: 42, Val: "Hello world!"}
	bytes, err := json.Marshal(p)
	a.NoError(err)

	a.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{{
		Data: bytes,
		Key:  []byte(fmt.Sprintf(`[%d]`, p.PK)),
	}}))
}

// This tests a case in which cdc-sink does not upsert all columns in
// the target table and where multiple updates to the same key are
// contained in the batch (which can happen in immediate mode). In this
// case, we'll see an error message that UPSERT cannot affect the same
// row multiple times.
//
// X-Ref: https://github.com/cockroachdb/cockroach/issues/44466
// X-Ref: https://github.com/cockroachdb/cockroach/pull/45372
func TestRepeatedKeysWithIgnoredColumns(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	type Payload struct {
		Pk0 int    `json:"pk0"`
		Val string `json:"val"`
	}
	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk0 INT PRIMARY KEY, ignored INT AS (1) STORED, val STRING)")
	if !a.NoError(err) {
		return
	}

	// Detect hopeful future case where UPSERT has the desired behavior.
	_, err = fixture.Pool.Exec(ctx,
		fmt.Sprintf("UPSERT INTO %s (pk0, val) VALUES ($1, $2), ($3, $4)", tbl.Name()),
		1, "1", 1, "1")
	if a.Error(err) {
		a.Contains(err.Error(), "cannot affect row a second time")
	} else {
		a.FailNow("the workaround is no longer necessary for this version of CRDB")
	}

	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

	p1 := Payload{Pk0: 10, Val: "First"}
	bytes1, err := json.Marshal(p1)
	a.NoError(err)

	p2 := Payload{Pk0: 10, Val: "Repeated"}
	bytes2, err := json.Marshal(p2)
	a.NoError(err)

	muts := []types.Mutation{
		{Data: bytes1, Key: []byte(fmt.Sprintf(`[%d]`, p1.Pk0)), Time: hlc.New(1, 1)},
		{Data: bytes2, Key: []byte(fmt.Sprintf(`[%d]`, p2.Pk0)), Time: hlc.New(1, 2)},
	}

	// Verify insertion.
	a.NoError(app.Apply(ctx, fixture.Pool, muts))

	count, err := sinktest.GetRowCount(ctx, fixture.Pool, tbl.Name())
	if a.NoError(err) && a.Equal(1, count) {
		row := fixture.Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT val FROM %s WHERE pk0 = $1", tbl.Name()), 10)
		var val string
		a.NoError(row.Scan(&val))
		a.Equal("Repeated", val)
	}
}

// Verify that user-defined enums with mixed-case identifiers work.
func TestUTDEnum(t *testing.T) {
	r := require.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context

	type Payload struct {
		PK  int    `json:"pk"`
		Val string `json:"val"`
	}

	_, err = fixture.Pool.Exec(ctx, fmt.Sprintf(
		`CREATE TYPE %s."MyEnum" AS ENUM ('foo', 'bar')`,
		fixture.TestDB.Ident()))
	r.NoError(err)

	tbl, err := fixture.CreateTable(ctx,
		fmt.Sprintf(`CREATE TABLE %%s (pk INT PRIMARY KEY, val %s."MyEnum")`,
			fixture.TestDB.Ident()))
	r.NoError(err)

	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	r.NoError(err)

	p := Payload{PK: 42, Val: "bar"}
	bytes, err := json.Marshal(p)
	r.NoError(err)

	r.NoError(app.Apply(ctx, fixture.Pool, []types.Mutation{{
		Data: bytes,
		Key:  []byte(fmt.Sprintf(`[%d]`, p.PK)),
	}}))
}

// Ensure that if stored computed columns are present, we don't
// try to write to them and that we correctly ignore those columns
// in incoming payloads.
func TestVirtualColumns(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	type Payload struct {
		A int `json:"a"`
		B int `json:"b"`
		C int `json:"c"`
		X int `json:"x,omitempty"`
	}
	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (a INT, b INT, c INT AS (a + b) STORED, PRIMARY KEY (a,b))")
	if !a.NoError(err) {
		return
	}

	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

	t.Run("computed-is-ignored", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		muts := []types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d, %d]`, p.A, p.B)),
		}}

		a.NoError(app.Apply(ctx, fixture.Pool, muts))
	})

	t.Run("unknown-still-breaks", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, X: -1}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		muts := []types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d, %d]`, p.A, p.B)),
		}}

		err = app.Apply(ctx, fixture.Pool, muts)
		if a.Error(err) {
			a.Contains(err.Error(), "unexpected columns")
		}
	})
}

type benchConfig struct {
	name          string
	batchSize     int
	cas, deadline bool
}

func BenchmarkApply(b *testing.B) {
	templates := []benchConfig{
		{name: "base"},
		{name: "cas", cas: true},
		{name: "deadline", deadline: true},
		{name: "cas+deadline", cas: true, deadline: true},
	}
	sizes := []int{1, 10, 100, 1_000, 10_000}

	tcs := make([]benchConfig, 0, len(templates)*len(sizes))
	for _, size := range sizes {
		for _, tmpl := range templates {
			// struct, not pointer type.
			tmpl.batchSize = size
			tcs = append(tcs, tmpl)
		}
	}

	for _, tc := range tcs {
		b.Run(tc.name, func(b *testing.B) {
			benchConditions(b, tc)
		})
	}
}

func benchConditions(b *testing.B, cfg benchConfig) {
	a := assert.New(b)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context

	tbl, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk UUID PRIMARY KEY, ver INT, ts TIMESTAMP)")
	if !a.NoError(err) {
		return
	}

	// Set up the apply instance, per the configuration.
	configData := apply.NewConfig()
	if cfg.cas {
		configData.CASColumns = []ident.Ident{ident.New("ver")}
	}
	if cfg.deadline {
		configData.Deadlines[ident.New("ts")] = time.Hour
	}
	a.NoError(fixture.Configs.Store(ctx, fixture.Pool, tbl.Name(), configData))
	changed, err := fixture.Configs.Refresh(ctx)
	a.True(changed)
	a.NoError(err)
	app, err := fixture.Appliers.Get(ctx, tbl.Name())
	if !a.NoError(err) {
		return
	}

	// Create a source of Mutatation data.
	muts := sinktest.MutationGenerator(ctx, 100000, 0)

	var loopTotal int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		batch := make([]types.Mutation, cfg.batchSize)
		loops := int64(0)

		for pb.Next() {
			for i := range batch {
				mut := <-muts
				batch[i] = mut
				atomic.AddInt64(&loopTotal, int64(len(mut.Data)))
			}

			// Applying a discarded mutation should never result in an error.
			if !a.NoError(app.Apply(context.Background(), fixture.Pool, batch)) {
				return
			}
			loops++
		}
	})

	// Use bytes as a throughput measure.
	b.SetBytes(atomic.LoadInt64(&loopTotal))
}
