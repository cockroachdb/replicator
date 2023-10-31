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
	"flag"
	"fmt"
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
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

	// Create a basic test fixture.
	fixture, cancel, err := base.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbSchema := fixture.TargetSchema.Schema()
	dbName := dbSchema.Idents(nil)[0] // Extract first name part.
	crdbPool := fixture.TargetPool

	pgPool, cancel, err := setupPGPool(dbName)
	if !a.NoError(err) {
		return
	}
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
	if !a.NoError(eg.Wait()) {
		return
	}

	pubNameRaw := publicationName(dbName).Raw()
	// Start the connection, to demonstrate that we can backfill pending mutations.
	cfg := &Config{
		BaseConfig: logical.BaseConfig{
			ApplyTimeout:  2 * time.Minute, // Increase to make using the debugger easier.
			ChaosProb:     fc.chaosProb,
			Immediate:     fc.immediate,
			RetryDelay:    time.Nanosecond,
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
	repl, cancelLoop, err := Start(ctx, cfg)
	if !a.NoError(err) {
		return
	}
	defer cancelLoop()

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
	if !a.NoError(err) {
		return
	}
	for _, tgt := range tgts {
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET v = 'updated'", tgt)); !a.NoError(err) {
			return
		}
	}
	if !a.NoError(tx.Commit(ctx)) {
		return
	}

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
	if !a.NoError(err) {
		return
	}
	for _, tgt := range tgts {
		if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE pk < 50", tgt)); !a.NoError(err) {
			return
		}
	}
	if !a.NoError(tx.Commit(ctx)) {
		return
	}

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

	// Build a long value for string and byte types.
	var sb strings.Builder
	for sb.Len() < 1<<16 {
		sb.WriteString("0123456789ABCDEF")
	}
	longString := sb.String()

	tcs := []struct {
		name   string
		values []any // We automatically test for NULL below.
	}{
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
	fixture, cancel, err := base.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbSchema := fixture.TargetSchema.Schema()
	dbName := dbSchema.Idents(nil)[0] // Extract first name part.
	crdbPool := fixture.TargetPool

	pgPool, cancel, err := setupPGPool(dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

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
	repl, cancelLoop, err := Start(ctx, &Config{
		BaseConfig: logical.BaseConfig{
			RetryDelay:    time.Nanosecond,
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
	defer cancelLoop()

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

	pubName := publicationName(database)

	if _, err := retConn.Exec(ctx,
		fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pubName),
	); err != nil {
		return nil, func() {}, err
	}

	if _, err := retConn.Exec(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
		pubName.Raw(),
	); err != nil {
		return nil, func() {}, err
	}

	return retConn, func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := retConn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", pubName.Raw())
		if err != nil {
			log.WithError(err).Error("could not drop database")
		}
		_, err = retConn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION %s", pubName))
		if err != nil {
			log.WithError(err).Error("could not drop publication")
		}
		retConn.Close()

		// Can't drop the default database from its own connection.
		_, err = baseConn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s", database))
		if err != nil {
			log.WithError(err).Error("could not drop database")
		}
		baseConn.Close()
		log.Trace("finished pg pool cleanup")
	}, nil
}
