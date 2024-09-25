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

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/sinktest/scripttest"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/workload"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	pgConnString = flag.String(
		"pgConn",
		"postgres://postgres:SoupOrSecret@127.0.0.1/",
		"connection string for PostgreSQL instance")
)

func TestMain(m *testing.M) {
	base.XXXSetSourceConn(*pgConnString)
	all.IntegrationMain(m, all.PostgreSQLName)
}

type fixtureConfig struct {
	chaos     bool
	partition bool // Generate source table names that don't exist in the target.
	rif       bool // Enable REPLICA IDENTITY FULL
	script    bool
}

// This is a general smoke-test of the logical replication feed that
// uses the workload checker to validate the replicated data.
func TestPGLogical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{})
	})
	t.Run("consistent-chaos", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{chaos: true})
	})
	t.Run("consistent-partition-script", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{partition: true, script: true})
	})
	t.Run("consistent-partition-rif-script", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{partition: true, rif: true, script: true})
	})
	t.Run("consistent-rif", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{rif: true})
	})
	t.Run("consistent-script", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{script: true})
	})
	t.Run("consistent-script-chaos", func(t *testing.T) {
		testPGLogical(t, &fixtureConfig{chaos: true, script: true})
	})
}

func testPGLogical(t *testing.T, fc *fixtureConfig) {
	const transactionCount = 1024
	a := assert.New(t)
	r := require.New(t)

	partitionCount := 1
	if fc.partition {
		partitionCount = 10
	}

	// This fixture points at the target. It will, eventually, be used
	// to validate that the target contains the data expected by the
	// workload checker.
	crdbFixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}
	ctx := crdbFixture.Context

	sourceSchema := crdbFixture.SourceSchema.Schema()
	sourceDB := sourceSchema.Idents(nil)[0] // Extract first name part.

	// These generators will act as sources of mutations to apply later
	// on and will then be used to validate the information in the
	// target.
	targetChecker, _, err := crdbFixture.NewWorkload(ctx, &all.WorkloadConfig{})
	r.NoError(err)
	sourceGenerators := make([]*workload.GeneratorBase, partitionCount)
	for i := range sourceGenerators {
		parent := ident.NewTable(sourceSchema, targetChecker.Parent.Name().Table())
		child := ident.NewTable(sourceSchema, targetChecker.Child.Name().Table())
		if fc.partition {
			part := i % partitionCount
			parent = ident.NewTable(parent.Schema(),
				ident.New(fmt.Sprintf("%s_P_%d", parent.Table().Raw(), part)))
			child = ident.NewTable(child.Schema(),
				ident.New(fmt.Sprintf("%s_P_%d", child.Table().Raw(), part)))
		}
		sourceGenerators[i] = workload.NewGeneratorBase(parent, child)
	}

	// Set up the source database with a replication publication and
	// create the workload table schema.
	pgPool := crdbFixture.SourcePool
	err = setupPublication(ctx, pgPool, sourceDB, "ALL TABLES")
	r.NoError(err)

	// Create the tables in the source database. We may simulate the
	// case where there are multiple partitioned source tables that fan
	// into a single target table. There's an extra test case here for
	// REPLICA IDENTITY FULL, which makes the source behave as though
	// all columns in the table are the primary key.
	for _, generator := range sourceGenerators {
		parent := generator.Parent
		child := generator.Child

		parentSQL, childSQL := all.WorkloadSchema(
			&all.WorkloadConfig{}, types.ProductPostgreSQL,
			parent, child)
		_, err = pgPool.ExecContext(ctx, parentSQL)
		r.NoError(err)
		_, err = pgPool.ExecContext(ctx, childSQL)
		r.NoError(err)
		if fc.rif {
			_, err := pgPool.ExecContext(ctx, fmt.Sprintf(
				`ALTER TABLE %s REPLICA IDENTITY FULL`, parent))
			r.NoError(err)
			_, err = pgPool.ExecContext(ctx, fmt.Sprintf(
				`ALTER TABLE %s REPLICA IDENTITY FULL`, child))
			r.NoError(err)
		}
	}

	// Create a new test fixture using the source database as the
	// target. This will allow us to use the apply package to insert the
	// generated workload data into the source tables.
	pgFixture, err := all.NewFixtureFromBase(crdbFixture.Swapped())
	r.NoError(err)
	acc := types.OrderedAcceptorFrom(pgFixture.ApplyAcceptor, pgFixture.Watchers)

	// Build the runtime configuration for the replication loop, which
	// we'll run as though it were being started from the command line
	// (i.e. it's going to dial the source and target itself, etc).
	pubNameRaw := publicationName(sourceDB).Raw()
	cfg := &Config{
		Sequencer: sequencer.Config{
			QuiescentPeriod: 100 * time.Millisecond,
		},
		Staging: sinkprod.StagingConfig{
			Schema: crdbFixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: crdbFixture.TargetPool.ConnectionString,
			},
			ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		},
		Publication:    pubNameRaw,
		Slot:           pubNameRaw,
		SourceConn:     *pgConnString + sourceDB.Raw(),
		StandbyTimeout: 100 * time.Millisecond,
		TargetSchema:   crdbFixture.TargetSchema.Schema(),
	}
	if fc.chaos {
		cfg.Sequencer.Chaos = 2
	}
	if fc.partition || fc.script {
		cfg.Script = script.Config{
			FS: scripttest.ScriptFSParentChild(
				targetChecker.Parent.Name(), targetChecker.Child.Name()),
			MainPath: "/testdata/logical_test_parent_child.ts",
		}
	}
	r.NoError(cfg.Preflight())

	// We control the lifecycle of the replication loop.
	connCtx := stopper.WithContext(ctx)
	repl, err := Start(connCtx, cfg)
	r.NoError(err)
	log.Info("started pglogical conn")

	// This will be filled in on the last insert transaction below, so
	// that we can monitor the replication loop until it proceeds to
	// this point in the WAL.
	var expectLSN pglogrepl.LSN

	// At this point, the source database has been configured with a
	// publication, replication slot and table schema. The replication
	// loop has started and established connections to the source and
	// the target. We can finally start generating data to apply.
	log.Info("starting to insert data into source")
	for i := range transactionCount {
		batch := &types.MultiBatch{}
		sourceGenerator := sourceGenerators[i%partitionCount]
		sourceGenerator.GenerateInto(batch, hlc.New(int64(i+1), i))

		tx, err := pgFixture.TargetPool.BeginTx(ctx, &sql.TxOptions{})
		r.NoError(err)
		r.NoError(acc.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{TargetQuerier: tx}))
		if i == transactionCount-1 {
			// On our final transaction, capture the LSN that
			// replication should advance to.
			r.NoError(tx.QueryRowContext(ctx,
				"SELECT pg_current_wal_insert_lsn()").Scan(&expectLSN))
		}
		r.NoError(tx.Commit())
	}
	log.Info("finished inserting data into source")

	// Merge source generators into the target checker.
	for _, gen := range sourceGenerators {
		targetChecker.CopyFrom(gen)
	}

	// Monitor the connection's committed walOffset. It should advance
	// beyond the insert point that was captured in the loop above.
	for {
		pos, posChanged := repl.Conn.walOffset.Get()
		if pos >= expectLSN {
			break
		}
		log.Infof("waiting for lsn: %s vs %s", pos, expectLSN)
		select {
		case <-posChanged:
		case <-ctx.Done():
			log.Errorf("timed out waiting for lsn: %s vs %s", pos, expectLSN)
			r.NoError(ctx.Err())
		}
	}

	// Validate the data in the target tables.
	r.True(targetChecker.CheckConsistent(ctx, t))

	// We need to wait for the connection to shut down, otherwise the
	// database cleanup callbacks (to drop the publication, etc.) from
	// the test code above can't succeed.
	connCtx.Stop(time.Minute)
	<-connCtx.Done()
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
	pgPool := fixture.SourcePool

	err = setupPublication(ctx, pgPool, dbName, "ALL TABLES")
	r.NoError(err)

	enumQ := fmt.Sprintf(`CREATE TYPE %s."Simple-Enum" AS ENUM ('foo', 'bar')`, dbSchema)
	_, err = crdbPool.ExecContext(ctx, enumQ)
	r.NoError(err, enumQ)
	_, err = pgPool.ExecContext(ctx, enumQ)
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
		if _, err := pgPool.ExecContext(ctx, schema); !a.NoErrorf(err, "PG %s", tc.name) {
			return
		}

		// Insert dummy data into the source in a single transaction.
		tx, err := pgPool.BeginTx(ctx, &sql.TxOptions{})
		if !a.NoError(err) {
			return
		}

		for valIdx, value := range tc.values {
			if _, err := tx.ExecContext(ctx,
				fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2::%s)`, tgt, tc.name), valIdx, value,
			); !a.NoErrorf(err, "%s %d %s", tc.name, valIdx, value) {
				return
			}
		}

		// Also insert a null value.
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s VALUES ($1, NULL::%s)`, tgt, tc.name), -1,
		); !a.NoError(err) {
			return
		}

		a.NoError(tx.Commit())
	}
	log.Info(tgts)
	pubNameRaw := publicationName(dbName).Raw()
	// Start the connection, to demonstrate that we can backfill pending mutations.
	cfg := &Config{
		Sequencer: sequencer.Config{
			QuiescentPeriod: 100 * time.Millisecond,
		},
		Staging: sinkprod.StagingConfig{
			Schema: fixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: crdbPool.ConnectionString,
			},
			ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		},
		Publication:    pubNameRaw,
		Slot:           pubNameRaw,
		SourceConn:     *pgConnString + dbName.Raw(),
		StandbyTimeout: 100 * time.Millisecond,
		TargetSchema:   dbSchema,
	}
	r.NoError(cfg.Preflight())
	repl, err := Start(fixture.Context, cfg)
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

func getCounterValue(t *testing.T, counter prometheus.Counter) int {
	r := require.New(t)
	var metric = &dto.Metric{}
	err := counter.Write(metric)
	r.NoError(err)
	return int(metric.Counter.GetValue())
}

// TestEmptyTransactions verify that we skip transactions.
func TestEmptyTransactions(t *testing.T) {

	a := assert.New(t)
	r := require.New(t)

	emptyTxnSeenStart := getCounterValue(t, emptyTransactionCount)

	applicableVersions := regexp.MustCompile("PostgreSQL 1[1234]")
	// Create a basic test fixture.
	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	dbSchema := fixture.TargetSchema.Schema()
	dbName := dbSchema.Idents(nil)[0] // Extract first name part.
	pgPool := fixture.SourcePool

	rows, err := pgPool.QueryContext(ctx, "select version()")
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

	if _, err := pgPool.ExecContext(ctx, schema); !a.NoErrorf(err, "PG %s", schema) {
		return
	}
	var localSchema = fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, v int)", localTable.Table())
	if _, err := pgPool.ExecContext(ctx, localSchema); !a.NoErrorf(err, "PG %s", localTable) {
		return
	}
	// setup replication for only the replTable
	err = setupPublication(ctx, pgPool, dbName, fmt.Sprintf(`TABLE %s`, replTable))
	r.NoError(err)

	pubNameRaw := publicationName(dbName).Raw()

	// Start the connection, to demonstrate that we can backfill pending mutations.
	cfg := &Config{
		Sequencer: sequencer.Config{
			QuiescentPeriod: 100 * time.Millisecond,
		},
		Staging: sinkprod.StagingConfig{
			Schema: fixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: crdbPool.ConnectionString,
			},
			ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		},
		Publication:    pubNameRaw,
		Slot:           pubNameRaw,
		SourceConn:     *pgConnString + dbName.Raw(),
		StandbyTimeout: 100 * time.Millisecond,
		TargetSchema:   dbSchema,
	}
	r.NoError(cfg.Preflight())
	repl, err := Start(fixture.Context, cfg)
	r.NoError(err)

	// Insert a value in the replTable
	if _, err := pgPool.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES (1,1)`, replTable)); !a.NoErrorf(err, "%s", replTable) {
		return
	}

	// Insert a value in the localTable
	if _, err := pgPool.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES (1,1)`, localTable)); !a.NoErrorf(err, "%s", replTable) {
		return
	}
	// Insert a value in the replTable
	if _, err := pgPool.ExecContext(ctx,
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
	pgPool := fixture.SourcePool

	err = setupPublication(ctx, pgPool, dbName, "ALL TABLES")
	r.NoError(err)

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
	if _, err := pgPool.ExecContext(ctx, schema); !a.NoErrorf(err, "PG %s", schema) {
		return
	}
	// Inserting an initial value. The t,s,j columns contains a large object that
	// will be toast-ed in the Postgres side.
	_, err = pgPool.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3, $4, $5)`, name),
		1, 0, longObj, longString, longString)
	r.NoError(err)
	// We update the "i" column few times without changing the j column.
	updates := 10
	for i := 1; i <= updates; i++ {
		if _, err := pgPool.ExecContext(ctx,
			fmt.Sprintf(`UPDATE %s SET i = $2 WHERE k = $1`, name), 1, i,
		); !a.NoErrorf(err, "%s", name) {
			return
		}
	}

	pubNameRaw := publicationName(dbName).Raw()

	cfg := &Config{
		Sequencer: sequencer.Config{
			QuiescentPeriod: 100 * time.Millisecond,
		},
		Staging: sinkprod.StagingConfig{
			Schema: fixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: crdbPool.ConnectionString,
			},
			ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		},
		Publication:    pubNameRaw,
		Slot:           pubNameRaw,
		SourceConn:     *pgConnString + dbName.Raw(),
		StandbyTimeout: 100 * time.Millisecond,
		TargetSchema:   dbSchema,
	}
	r.NoError(cfg.Preflight())
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
			if errors.Is(err, sql.ErrNoRows) {
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
		// Verify that we actually see empty toasted column markers
		a.NotZero(getCounterValue(t, unchangedToastedColumns))

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

func setupPublication(
	ctx *stopper.Context, pool *types.SourcePool, database ident.Ident, scope string,
) error {
	pubName := publicationName(database)
	if _, err := pool.ExecContext(ctx,
		fmt.Sprintf("CREATE PUBLICATION %s FOR %s", pubName, scope),
	); err != nil {
		return err
	}

	if _, err := pool.ExecContext(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
		pubName.Raw(),
	); err != nil {
		return err
	}
	ctx.Defer(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := pool.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", pubName.Raw())
		if err != nil {
			log.WithError(err).Error("could not drop replication slot")
		}
		_, err = pool.ExecContext(ctx, fmt.Sprintf("DROP PUBLICATION %s", pubName))
		if err != nil {
			log.WithError(err).Error("could not drop publication")
		}
		log.Trace("finished pg pool cleanup")
	})
	return nil
}
