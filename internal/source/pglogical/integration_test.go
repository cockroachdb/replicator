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

package pglogical

// The tests in this package rely on the Docker Compose configurations
// in the .github directory.

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/scripttest"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	pgConnString = flag.String(
		"pgConn",
		"postgres://postgres:SoupOrSecret@127.0.0.1/",
		"connection string for PostgreSQL instance")
)

func TestMain(m *testing.M) {
	log.SetLevel(log.TraceLevel)
	all.IntegrationMain(m, all.PostgreSQLName)
}

type fixtureConfig struct {
	backfill  bool
	chaosProb float32
	immediate bool
	script    bool
}

// This is a general smoke-test of the logical replication feed.
//
// The probabilities are chosen to make the tests pass within a
// reasonable timeframe, given the large number of rows that we insert.
func TestPGLogical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testPGLogical(t, &fixtureConfig{}) })
	t.Run("consistent-backfill", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{backfill: true})
	})
	t.Run("consistent-backfill-chaos", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{backfill: true, chaosProb: 0.0005})
	})
	t.Run("consistent-chaos", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{chaosProb: 0.0005})
	})
	t.Run("consistent-script", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{script: true})
	})
	t.Run("immediate", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{immediate: true})
	})
	t.Run("immediate-chaos", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{immediate: true, chaosProb: 0.0005})
	})
	t.Run("immediate-script", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{immediate: true, script: true})
	})
}

func testPGLogical(t *testing.T, fc *fixtureConfig) {
	a := assert.New(t)
	r := require.New(t)
	// Create a basic test fixture.
	fixture, err := base.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context
	dbSchema := fixture.TargetSchema.Schema()
	dbName := dbSchema.Idents(nil)[0] // Extract first name part.
	crdbPool := fixture.TargetPool

	pgPool, cancel, err := setupPGPool(dbName)
	r.NoError(err)

	defer cancel()
	cancel, err = setupPublication(ctx, pgPool, dbName, "ALL TABLES")
	r.NoError(err)
	defer cancel()
	// Create the schema in both locations.
	var tgts []ident.Table
	if fc.script {
		// The logical_test script rig is only set up to deal with one
		// table. We really just want to verify that the end-to-end
		// wiring is in place.
		tgts = []ident.Table{
			ident.NewTable(dbSchema, ident.New("script_tbl")),
		}
	} else {
		tgts = []ident.Table{
			ident.NewTable(dbSchema, ident.New("t1")),
			ident.NewTable(dbSchema, ident.New("t2")),
		}
	}

	var crdbCol string
	for _, tgt := range tgts {
		var crdbSchema, pgSchema string
		pgSchema = fmt.Sprintf(`CREATE TABLE %s (pk INT PRIMARY KEY, v TEXT)`, tgt)
		crdbSchema = pgSchema
		crdbCol = "v"
		if fc.script {
			// Ensure that script is wired up by renaming a column.
			crdbSchema = fmt.Sprintf(`CREATE TABLE %s (pk INT PRIMARY KEY, v_mapped TEXT)`, tgt)
			crdbCol = "v_mapped"
		}
		if _, err := pgPool.Exec(ctx, pgSchema); !a.NoError(err) {
			return
		}
		if _, err := crdbPool.ExecContext(ctx, crdbSchema); !a.NoError(err) {
			return
		}
	}

	// We want enough rows here to make sure that the batching and
	// coalescing logic gets exercised.
	rowCount := 10 * batches.Size()
	keys := make([]int, rowCount)
	vals := make([]string, rowCount)

	// Insert data into source tables with overlapping transactions.
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(128)
	for _, tgt := range tgts {
		tgt := tgt // Capture
		for i := 0; i < len(vals); i += 2 {
			i := i // Capture
			eg.Go(func() error {
				keys[i] = i
				keys[i+1] = i + 1
				vals[i] = fmt.Sprintf("v=%d", i)
				vals[i+1] = fmt.Sprintf("v=%d", i+1)

				tx, err := pgPool.Begin(egCtx)
				if err != nil {
					return err
				}
				_, err = tx.Exec(egCtx,
					fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tgt),
					keys[i], vals[i],
				)
				if err != nil {
					return err
				}
				_, err = tx.Exec(egCtx,
					fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tgt),
					keys[i+1], vals[i+1],
				)
				if err != nil {
					return err
				}
				return tx.Commit(egCtx)
			})
		}
	}
	r.NoError(err)

	pubNameRaw := publicationName(dbName).Raw()
	// Start the connection, to demonstrate that we can backfill pending mutations.
	cfg := &Config{
		BaseConfig: logical.BaseConfig{
			ApplyTimeout:   2 * time.Minute, // Increase to make using the debugger easier.
			ChaosProb:      fc.chaosProb,
			Immediate:      fc.immediate,
			RetryDelay:     time.Millisecond,
			StagingSchema:  fixture.StagingDB.Schema(),
			TargetConn:     crdbPool.ConnectionString,
			StandbyTimeout: 100 * time.Millisecond,
		},
		LoopConfig: logical.LoopConfig{
			LoopName:     "pglogicaltest",
			TargetSchema: dbSchema,
		},
		Publication: pubNameRaw,
		Slot:        pubNameRaw,
		SourceConn:  *pgConnString + dbName.Raw(),
	}
	if fc.backfill {
		cfg.BackfillWindow = time.Minute
	} else {
		cfg.BackfillWindow = 0
	}
	if fc.script {
		cfg.ScriptConfig = script.Config{
			FS:       scripttest.ScriptFSFor(tgts[0]),
			MainPath: "/testdata/logical_test.ts",
		}
	}
	repl, err := Start(ctx, cfg)
	r.NoError(err)

	// Wait for backfill.
	for _, tgt := range tgts {
		for {
			var count int
			if err := crdbPool.QueryRowContext(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s", tgt)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Trace("backfill count", count)
			if count == rowCount {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Let's perform an update in a single transaction.
	tx, err := pgPool.Begin(ctx)
	r.NoError(err)
	for _, tgt := range tgts {
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET v = 'updated'", tgt)); !a.NoError(err) {
			return
		}
	}
	r.NoError(tx.Commit(ctx))

	// Wait for the update to propagate.
	for _, tgt := range tgts {
		for {
			var count int
			if err := crdbPool.QueryRowContext(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = 'updated'", tgt, crdbCol)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Trace("update count", count)
			if count == rowCount {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Delete some rows.
	tx, err = pgPool.Begin(ctx)
	r.NoError(err)
	for _, tgt := range tgts {
		if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE pk < 50", tgt)); !a.NoError(err) {
			return
		}
	}
	r.NoError(tx.Commit(ctx))

	// Wait for the deletes to propagate.
	for _, tgt := range tgts {
		for {
			var count int
			if err := crdbPool.QueryRowContext(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = 'updated'", tgt, crdbCol)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Trace("delete count", count)
			if count == rowCount-50 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	sinktest.CheckDiagnostics(ctx, t, repl.Diagnostics)

	ctx.Stop(time.Second)
	a.NoError(ctx.Wait())
}

// https://www.postgresql.org/docs/current/datatype.html
func TestDataTypes(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	// Build a long value for string and byte types.
	var sb strings.Builder
	for sb.Len() < 1<<16 {
		sb.WriteString("0123456789ABCDEF")
	}
	longString := sb.String()

	type tc struct {
		name   string
		values []any // We automatically test for NULL below.
	}
	tcs := []tc{
		{`bigint`, []any{0, -1, 112358}},
		// bigserial doesn't exist as a reifiable type in pg?
		{`bit`, []any{"10010101"}},
		{`bit varying`, []any{"10010101"}},
		{`boolean`, []any{true, false}},
		// box
		{`bytea`, []any{"", "HELLO WORLD!", []byte(longString)}},
		{`character`, []any{"", "HELLO WORLD!", longString}},
		{`character varying`, []any{"", "HELLO WORLD!", longString}},
		// cidr not implemented: https://github.com/cockroachdb/cockroach/issues/18846
		// circle
		{`date`, []any{"2020/02/02", time.Date(2022, 02, 22, 0, 0, 0, 0, time.UTC)}},
		{`double precision`, []any{0.0, -1.1, 1.1}},
		{`inet`, []any{"127.0.0.1"}},
		{`integer`, []any{-1, 0, 112358}},
		{`interval`, []any{time.Duration(0), time.Hour}},
		{`json`, []any{"null", `{"hello":"world"}`, fmt.Sprintf(`{"long":%q}`, longString)}},
		{`jsonb`, []any{"null", `{"hello":"world"}`, fmt.Sprintf(`{"long":%q}`, longString)}},
		// line
		// lseg
		// macaddr not implemented: https://github.com/cockroachdb/cockroach/issues/45813
		// money not implemented: https://github.com/cockroachdb/cockroach/issues/41578
		{`numeric`, []any{"0", 55, 5.56}},
		// path
		// pg_lsn
		// pg_snapshot
		// point
		// polygon
		{`real`, []any{0.0, -1.1, 1.1}},
		{`smallint`, []any{0.0, -1, 1}},
		{`text`, []any{``, `Hello World!`, longString}},
		{`time`, []any{"16:20"}},
		{`time with time zone`, []any{"16:20"}},
		// tsquery
		// tsvector
		// txid_snapshot
		{`uuid`, []any{uuid.New()}},
		// xml
	}

	// Create a basic test fixture.
	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	dbSchema := fixture.TargetSchema.Schema()
	dbName := dbSchema.Idents(nil)[0] // Extract first name part.
	crdbPool := fixture.TargetPool

	pgPool, cancel, err := setupPGPool(dbName)
	r.NoError(err)
	defer cancel()

	cancel, err = setupPublication(ctx, pgPool, dbName, "ALL TABLES")
	r.NoError(err)
	defer cancel()

	enumQ := fmt.Sprintf(`CREATE TYPE %s."Simple-Enum" AS ENUM ('foo', 'bar')`, dbSchema)
	_, err = crdbPool.ExecContext(ctx, enumQ)
	r.NoError(err, enumQ)
	_, err = pgPool.Exec(ctx, enumQ)
	r.NoError(err, enumQ)
	tcs = append(tcs,
		tc{
			fmt.Sprintf(`%s."Simple-Enum"`, dbSchema),
			[]any{"foo", "bar"},
		},
		tc{
			fmt.Sprintf(`%s."Simple-Enum"[]`, dbSchema),
			[]any{"{foo}", "{bar}"},
		},
	)

	// Create a dummy table for each type
	tgts := make([]ident.Table, len(tcs))
	for idx, tc := range tcs {

		// Create the schema in both locations.
		var schema = fmt.Sprintf("CREATE TABLE %%s (k INT PRIMARY KEY, v %s)", tc.name)
		ti, err := fixture.CreateTargetTable(ctx, schema)
		if !a.NoErrorf(err, "CRDB %s", tc.name) {
			return
		}
		tgt := ti.Name()
		tgts[idx] = tgt

		// Substitute the created table name to send to postgres.
		schema = fmt.Sprintf(schema, tgt)
		if _, err := pgPool.Exec(ctx, schema); !a.NoErrorf(err, "PG %s", tc.name) {
			return
		}

		// Insert dummy data into the source in a single transaction.
		tx, err := pgPool.Begin(ctx)
		if !a.NoError(err) {
			return
		}

		for valIdx, value := range tc.values {
			if _, err := tx.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2::%s)`, tgt, tc.name), valIdx, value,
			); !a.NoErrorf(err, "%s %d %s", tc.name, valIdx, value) {
				return
			}
		}

		// Also insert a null value.
		if _, err := tx.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s VALUES ($1, NULL::%s)`, tgt, tc.name), -1,
		); !a.NoError(err) {
			return
		}

		a.NoError(tx.Commit(ctx))
	}
	log.Info(tgts)
	pubNameRaw := publicationName(dbName).Raw()
	// Start the connection, to demonstrate that we can backfill pending mutations.
	repl, err := Start(fixture.Context, &Config{
		BaseConfig: logical.BaseConfig{
			RetryDelay:    time.Millisecond,
			StagingSchema: fixture.StagingDB.Schema(),
			TargetConn:    crdbPool.ConnectionString,
		},
		LoopConfig: logical.LoopConfig{
			LoopName:     "pglogicaltest",
			TargetSchema: dbSchema,
		},
		Publication: pubNameRaw,
		Slot:        pubNameRaw,
		SourceConn:  *pgConnString + dbName.Raw(),
	})
	if !a.NoError(err) {
		return
	}

	// Wait for rows to show up.
	for idx, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			var count int
			for count == 0 {
				count, err = base.GetRowCount(ctx, crdbPool, tgts[idx])
				if !a.NoError(err) {
					return
				}
				if count == 0 {
					time.Sleep(100 * time.Millisecond)
				}
			}
			// Expect the test data and a row with a NULL value.
			a.Equalf(len(tc.values)+1, count, "mismatch in %s", tgts[idx])
		})
	}

	sinktest.CheckDiagnostics(ctx, t, repl.Diagnostics)

	ctx.Stop(time.Second)
	a.NoError(ctx.Wait())
}

func TestEmptyTransactions(t *testing.T) {
	t.Run("donot-skip-empty", func(t *testing.T) {
		testEmptyTransactions(t, false)
	})
	t.Run("skip-empty", func(t *testing.T) {
		testEmptyTransactions(t, true)
	})
}

func getCounterValue(t *testing.T, counter prometheus.Counter) int {
	r := require.New(t)
	var metric = &dto.Metric{}
	err := counter.Write(metric)
	r.NoError(err)
	return int(metric.Counter.GetValue())
}

// testEmptyTransactions must be run sequentially
// we are tracking metrics to verify that we see,
// and skip if required, empty transactions.
func testEmptyTransactions(t *testing.T, skipEmpty bool) {

	a := assert.New(t)
	r := require.New(t)

	emptyTxnSeenStart := getCounterValue(t, emptyTransactionCount)
	emptyTxnSkippedStart := getCounterValue(t, skippedEmptyTransactionCount)

	applicableVersions := regexp.MustCompile("PostgreSQL 1[1234]")
	// Create a basic test fixture.
	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	dbSchema := fixture.TargetSchema.Schema()
	dbName := dbSchema.Idents(nil)[0] // Extract first name part.

	pgPool, cancel, err := setupPGPool(dbName)
	r.NoError(err)
	defer cancel()

	rows, err := pgPool.Query(ctx, "select version()")
	r.NoError(err)
	defer rows.Close()
	var pgVersion string
	if rows.Next() {
		rows.Scan(&pgVersion)
	}
	if !applicableVersions.MatchString(pgVersion) {
		t.Skipf("not applicable for Postgres >= 15. Version: %s", fixture.SourcePool.Version)
	}
	rows.Close()

	crdbPool := fixture.TargetPool
	replTable := ident.NewTable(dbSchema, ident.New("replTable"))
	localTable := ident.NewTable(dbSchema, ident.New("localTable"))

	// Create replTable in both locations.
	var schema = fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, v int)", replTable)
	_, err = crdbPool.ExecContext(ctx, schema)
	if !a.NoErrorf(err, "CRDB %s", replTable) {
		return
	}

	if _, err := pgPool.Exec(ctx, schema); !a.NoErrorf(err, "PG %s", schema) {
		return
	}
	var localSchema = fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, v int)", localTable.Table())
	if _, err := pgPool.Exec(ctx, localSchema); !a.NoErrorf(err, "PG %s", localTable) {
		return
	}
	// setup replication for only the replTable
	cancel, err = setupPublication(ctx, pgPool, dbName, fmt.Sprintf(`TABLE %s`, replTable))
	r.NoError(err)
	defer cancel()

	pubNameRaw := publicationName(dbName).Raw()

	// Start the connection, to demonstrate that we can backfill pending mutations.
	repl, err := Start(fixture.Context, &Config{
		BaseConfig: logical.BaseConfig{
			RetryDelay:    time.Millisecond,
			StagingSchema: fixture.StagingDB.Schema(),
			TargetConn:    crdbPool.ConnectionString,
		},
		LoopConfig: logical.LoopConfig{
			LoopName:     "pglogicaltest",
			TargetSchema: dbSchema,
		},
		Publication:           pubNameRaw,
		SkipEmptyTransactions: skipEmpty,
		Slot:                  pubNameRaw,
		SourceConn:            *pgConnString + dbName.Raw(),
	})
	r.NoError(err)

	// Insert a value in the replTable
	if _, err := pgPool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES (1,1)`, replTable)); !a.NoErrorf(err, "%s", replTable) {
		return
	}

	// Insert a value in the localTable
	if _, err := pgPool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES (1,1)`, localTable)); !a.NoErrorf(err, "%s", replTable) {
		return
	}
	// Insert a value in the replTable
	if _, err := pgPool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES (2,2)`, replTable)); !a.NoErrorf(err, "%s", replTable) {
		return
	}

	// Waiting for the values in the replTable to show up
	var count int
	for count < 2 {
		count, err = base.GetRowCount(ctx, crdbPool, replTable)
		if !a.NoError(err) {
			return
		}
		if count == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	a.Equal(2, count)
	// Check emptyTransactionCount, we should see one empty transaction.
	a.Equal(emptyTxnSeenStart+1, getCounterValue(t, emptyTransactionCount))
	sinktest.CheckDiagnostics(ctx, t, repl.Diagnostics)
	// If we are skipping transactions, check the skippedEmptyTransactionCount.
	if skipEmpty {
		a.Equal(emptyTxnSkippedStart+1, getCounterValue(t, skippedEmptyTransactionCount))
	}
	ctx.Stop(time.Second)
	a.NoError(ctx.Wait())

}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestToast(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	// We need a long string that is not compressible.
	longString := randString(1 << 12)
	longObj, err := json.Marshal(longString)
	r.NoError(err)
	// Create a basic test fixture.
	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	dbSchema := fixture.TargetSchema.Schema()
	dbName := dbSchema.Idents(nil)[0] // Extract first name part.
	crdbPool := fixture.TargetPool

	pgPool, cancel, err := setupPGPool(dbName)
	r.NoError(err)
	defer cancel()

	cancel, err = setupPublication(ctx, pgPool, dbName, "ALL TABLES")
	r.NoError(err)
	defer cancel()

	name := ident.NewTable(dbSchema, ident.New("toast"))
	// Create the schema in both locations.
	schema := fmt.Sprintf(`
	CREATE TABLE %s
	  (k INT PRIMARY KEY,
	   i int,
	   j jsonb,
	   s string,
	   t text,
	   deleted bool not null default false)`, name)
	if _, err := crdbPool.ExecContext(ctx, schema); !a.NoError(err) {
		return
	}
	schema = fmt.Sprintf(`
	CREATE TABLE %s
	  (k INT PRIMARY KEY,
	   i int,
	   j jsonb,
	   s text,
	   t text,
	   deleted bool not null default false)`, name)
	if _, err := pgPool.Exec(ctx, schema); !a.NoErrorf(err, "PG %s", schema) {
		return
	}
	// Inserting an initial value. The t,s,j columns contains a large object that
	// will be toast-ed in the Postgres side.
	_, err = pgPool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3, $4, $5)`, name),
		1, 0, longObj, longString, longString)
	r.NoError(err)
	// We update the "i" column few times without changing the j column.
	updates := 10
	for i := 1; i <= updates; i++ {
		if _, err := pgPool.Exec(ctx,
			fmt.Sprintf(`UPDATE %s SET i = $2 WHERE k = $1`, name), 1, i,
		); !a.NoErrorf(err, "%s", name) {
			return
		}
	}

	pubNameRaw := publicationName(dbName).Raw()

	cfg := &Config{
		BaseConfig: logical.BaseConfig{
			RetryDelay:    time.Millisecond,
			StagingSchema: fixture.StagingDB.Schema(),
			TargetConn:    crdbPool.ConnectionString,
			Immediate:     true,
		},
		LoopConfig: logical.LoopConfig{
			LoopName:     "pglogicaltest",
			TargetSchema: dbSchema,
		},
		Publication:    pubNameRaw,
		Slot:           pubNameRaw,
		SourceConn:     *pgConnString + dbName.Raw(),
		ToastedColumns: true,
	}
	repl, err := Start(fixture.Context, cfg)

	if !a.NoError(err) {
		return
	}

	t.Run("toast", func(t *testing.T) {
		a := assert.New(t)
		count := 1
		for count < updates {
			row := crdbPool.QueryRowContext(ctx, fmt.Sprintf("SELECT i from %s limit 1", name))
			err = row.Scan(&count)
			if err == sql.ErrNoRows {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			r.NoError(err)
			if count < updates {
				time.Sleep(100 * time.Millisecond)
			}
		}
		a.Equalf(updates, count, "mismatch in %s", name)
		var text sql.NullString
		var st sql.NullString
		var json []byte
		row := crdbPool.QueryRowContext(ctx, fmt.Sprintf("SELECT j,s,t from %s where k=1", name))
		err = row.Scan(&json, &st, &text)
		// Verify that the toasted column has the initial value
		a.Equal(longObj, json)
		a.Equal(longString, st.String)
		a.Equal(longString, text.String)
		// Verify that we actually seen empty toasted column markers
		a.Equal(updates*3, getCounterValue(t, unchangedToastedColumns))

	})

	sinktest.CheckDiagnostics(ctx, t, repl.Diagnostics)
	ctx.Stop(time.Second)
	a.NoError(ctx.Wait())
}

// Allowable publication slot names are a subset of allowable
// database names, so we need to replace the must-quote dashes in
// the database name.
func publicationName(database ident.Ident) ident.Ident {
	return ident.New(strings.ReplaceAll(database.Raw(), "-", "_"))
}

func setupPGPool(database ident.Ident) (*pgxpool.Pool, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	baseConn, err := pgxpool.New(ctx, *pgConnString)
	if err != nil {
		return nil, func() {}, err
	}

	if _, err := baseConn.Exec(ctx,
		fmt.Sprintf("CREATE DATABASE %s", database),
	); err != nil {
		return nil, func() {}, err
	}

	// Open the pool, using the newly-created database.
	next := baseConn.Config().Copy()
	next.ConnConfig.Database = database.Raw()
	retConn, err := pgxpool.NewWithConfig(ctx, next)
	if err != nil {
		return nil, func() {}, err
	}

	return retConn, func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Can't drop the default database from its own connection.
		_, err = baseConn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s", database))
		if err != nil {
			log.WithError(err).Error("could not drop database")
		}
		baseConn.Close()
		log.Trace("finished pg pool cleanup")
	}, nil
}

func setupPublication(
	ctx context.Context, retConn *pgxpool.Pool, database ident.Ident, scope string,
) (func(), error) {
	pubName := publicationName(database)
	if _, err := retConn.Exec(ctx,
		fmt.Sprintf("CREATE PUBLICATION %s FOR %s", pubName, scope),
	); err != nil {
		return func() {}, err
	}

	if _, err := retConn.Exec(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
		pubName.Raw(),
	); err != nil {
		return func() {}, err
	}
	return func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := retConn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", pubName.Raw())
		if err != nil {
			log.WithError(err).Error("could not drop replication slot")
		}
		_, err = retConn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION %s", pubName))
		if err != nil {
			log.WithError(err).Error("could not drop publication")
		}
		retConn.Close()

		log.Trace("finished pg pool cleanup")
	}, nil
}
