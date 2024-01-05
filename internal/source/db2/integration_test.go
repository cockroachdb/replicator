// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//go:build db2 && cgo
// +build db2,cgo

package db2

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/scripttest"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	_ "github.com/ibmdb/go_ibm_db"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultSourceConn = "db2://db2inst1:SoupOrSecret@localhost:50000/TESTDB"

// TODO (silvano): provide a configuration option.
var cdcSchema = ident.MustSchema(ident.New("ASNCDC"))

type fixtureConfig struct {
	backfill  bool
	chaosProb float32
	immediate bool
	script    bool
}

type tableInfo struct {
	name          ident.Ident
	sourceColName ident.Ident
	sourceColType string
	targetColName ident.Ident
	targetColType string
}

func TestMain(m *testing.M) {
	all.IntegrationMain(m, all.DB2Name)
}
func TestB2Logical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testDB2Logical(t, &fixtureConfig{}) })
	t.Run("consistent-backfill", func(t *testing.T) {
		testDB2Logical(t, &fixtureConfig{backfill: true})
	})
	t.Run("consistent-backfill-chaos", func(t *testing.T) {
		testDB2Logical(t, &fixtureConfig{backfill: true, chaosProb: 0.0005})
	})
	t.Run("consistent-chaos", func(t *testing.T) {
		testDB2Logical(t, &fixtureConfig{chaosProb: 0.0005})
	})
	t.Run("consistent-script", func(t *testing.T) {
		testDB2Logical(t, &fixtureConfig{script: true})
	})
	t.Run("immediate", func(t *testing.T) {
		testDB2Logical(t, &fixtureConfig{immediate: true})
	})
	t.Run("immediate-chaos", func(t *testing.T) {
		testDB2Logical(t, &fixtureConfig{immediate: true, chaosProb: 0.0005})
	})
	t.Run("immediate-script", func(t *testing.T) {
		testDB2Logical(t, &fixtureConfig{immediate: true, script: true})
	})
}

func testDB2Logical(t *testing.T, fc *fixtureConfig) {
	r := require.New(t)
	// Create a basic test fixture.
	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context
	testDB2LogicalInt(t, fixture, fc)
	ctx.Stop(time.Second)
	r.NoError(ctx.Wait())
}
func testDB2LogicalInt(t *testing.T, fixture *base.Fixture, fc *fixtureConfig) {
	a := assert.New(t)
	r := require.New(t)
	ctx := fixture.Context
	// Using uppercase for consistency
	tableName := ident.New("T")
	targetColName := "V"
	if fc.script {
		targetColName = "v_mapped"
	}
	table := tableInfo{
		name:          tableName,
		sourceColName: ident.New("V"),
		sourceColType: "varchar(20)",
		targetColName: ident.New(targetColName),
		targetColType: "string",
	}

	repl, config, err := createRepl(ctx, fixture, fc, tableName)
	r.NoError(err)
	defer repl.cleanUp(ctx)

	tableMap, cancel, err := repl.createTables(ctx, table)
	r.NoError(err)
	defer cancel()
	start := time.Now()
	// CDC needs to be initialized again on DB2.
	err = repl.cdcReinit(ctx)
	r.NoError(err)
	log.Infof("Restarted CDC in %d ms", time.Since(start).Milliseconds())
	const rowCount = 512
	values := make([]any, rowCount)
	for i := 0; i < rowCount; i++ {
		values[i] = fmt.Sprintf("v=%d", i)
	}
	// Insert data into source table.
	start = time.Now()
	err = repl.insertValues(ctx, tableMap.source, values, false)
	r.NoError(err)
	log.Infof("Inserted rows in %s. %d ms", tableMap.source, time.Since(start).Milliseconds())
	// Start logical loop
	loop, err := Start(ctx, config)
	r.NoError(err)
	start = time.Now()
	for {
		var count int
		if err := repl.target.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tableMap.target)).Scan(&count); !a.NoError(err) {
			return
		}
		if count == rowCount {
			break
		}
		time.Sleep(1 * time.Second)
	}
	log.Infof("Rows replicated %s. %d ms", tableMap.source, time.Since(start).Milliseconds())
	_, err = repl.source.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET %s= 'updated'`, tableMap.source, table.sourceColName))
	r.NoError(err)
	// Wait for the update to propagate.
	for {
		var count int
		if err := repl.target.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = 'updated'", tableMap.target, table.targetColName)).Scan(&count); !a.NoError(err) {
			return
		}
		if count == rowCount {
			break
		}
		time.Sleep(1 * time.Second)
	}

	_, err = repl.source.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE "PK" < 50`, tableMap.source))
	r.NoError(err)
	// Wait for the deletes to propagate.
	for {
		var count int
		if err := repl.target.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = 'updated'", tableMap.target, table.targetColName)).Scan(&count); !a.NoError(err) {
			return
		}
		if count == rowCount-50 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	sinktest.CheckDiagnostics(ctx, t, loop.Diagnostics)

}

func TestDataTypes(t *testing.T) {
	r := require.New(t)
	// Create a basic test fixture.
	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context
	testDataTypes(t, fixture)
	ctx.Stop(time.Second)
	r.NoError(ctx.Wait())
}

func testDataTypes(t *testing.T, fixture *base.Fixture) {
	r := require.New(t)
	// Build a long value for string and byte types.
	var sb strings.Builder
	shortString := "0123456789ABCDEF"
	for sb.Len() < 1<<16 {
		sb.WriteString(shortString)
	}
	longString := sb.String()
	ctx := fixture.Context
	repl, config, err := createRepl(ctx, fixture, &fixtureConfig{immediate: false}, ident.Ident{})
	r.NoError(err)
	defer repl.cleanUp(ctx)

	tcs := []struct {
		source string
		size   string
		crdb   string
		values []any // We automatically test for NULL below.
	}{

		// BIGINT(size)
		{`bigint`, ``, `bigint`, []any{0, -1, 112358}},
		// BINARY(size)
		{`binary`, `(128)`, `bytes`, []any{[]byte(shortString)}},

		// BLOB(size)
		{`blob`, `(65536)`, `blob`, []any{[]byte(longString)}},

		// BOOLEAN
		// Currently SQL Replication on Db2 does not support BOOLEAN.
		//{`boolean`, ``, `int`, []any{true, false}},
		// CHAR(size)
		{`char`, `(128)`, `string`, []any{[]byte(shortString)}},
		// CLOB(size)
		{`clob`, `(128)`, `string`, []any{[]byte(shortString)}},
		// DATE
		{`date`, ``, `date`, []any{"1000-01-01", "9999-12-31"}},
		// DATETIME(fsp)
		{`datetime`, ``, `timestamp`, []any{"1000-01-01 00:00:00", "9999-12-31 23:59:59"}},

		{`dbclob`, `(128)`, `string`, []any{shortString}},

		// DECFLOAT
		// DECIMAL(size, d)
		{`decfloat`, ``, `decimal`, []any{math.E, math.Phi, math.Pi}},
		{`decimal`, `(11,10)`, `decimal`, []any{math.E, math.Phi, math.Pi}},

		{`double`, ``, `float`, []any{math.E, math.Phi, math.Pi}},
		{`float`, ``, `float`, []any{math.E, math.Phi, math.Pi}},
		// INT(size)
		// INTEGER(size)
		{`int`, ``, `int`, []any{0, -1, 112358}},

		{`real`, ``, `float`, []any{math.E, math.Phi, math.Pi}},
		// SMALLINT(size)
		{`smallint`, ``, `int`, []any{0, -1, 127}},

		// TIME(fsp)
		{`time`, ``, `time`, []any{"16:20:00"}},
		// TIMESTAMP(fsp)
		{`timestamp`, ``, `timestamp`, []any{"1970-01-01 00:00:01", "2038-01-19 03:14:07"}},
		// VARBINARY(size)
		{`varbinary`, `(128)`, `bytes`, []any{[]byte(shortString)}},
		// VARCHAR(size)
		{`varchar`, `(128)`, `text`, []any{shortString}},

		// Sentinel
		{`char`, `(4)`, `text`, []any{`done`}},

		// XML replication is not supported on LUW
		//{`xml`, ``, `text`, `<t>a</t>`},
	}

	// Create a dummy table for each type.
	tables := make([]*tableMapping, len(tcs))
	for idx, tc := range tcs {
		tbl := tableInfo{
			name:          ident.New(fmt.Sprintf("tgt_%s_%d", tc.source, idx)),
			sourceColName: ident.New("V"),
			sourceColType: tc.source + tc.size,
			targetColName: ident.New("V"),
			targetColType: tc.crdb,
		}
		tgt, cancel, err := repl.createTables(ctx, tbl)
		r.NoError(err)
		tables[idx] = tgt
		defer cancel()
	}

	// CDC needs to be initialized again on DB2.
	err = repl.cdcReinit(ctx)
	r.NoError(err)

	// Insert data in the source tables.
	for idx, tc := range tcs {
		err := repl.insertValues(ctx, tables[idx].source, tc.values, true)
		r.NoError(err)
	}

	// Wait for the CDC source tables to be fully populated.
	lastidx := len(tcs) - 1
	expected := len(tcs[lastidx].values) + 1
	var count int
	query := fmt.Sprintf("SELECT count(*) FROM %s", tables[lastidx].sourceCdc.Raw())
	for count < expected {
		err := repl.source.QueryRowContext(ctx, fmt.Sprintf(query)).Scan(&count)
		r.NoError(err)
		if count < expected {
			time.Sleep(1 * time.Second)
		}
	}

	// Start logical loop
	loop, err := Start(ctx, config)
	r.NoError(err)

	// Wait for rows to show up.
	for idx, tc := range tcs {
		tc := tc
		t.Run(tc.source, func(t *testing.T) {
			expected := len(tc.values) + 1
			a := assert.New(t)
			var count int
			for count < expected {
				if err := repl.target.QueryRowContext(ctx,
					fmt.Sprintf("SELECT count(*) FROM %s", tables[idx].target)).Scan(&count); !a.NoError(err) {
					return
				}
				if count < expected {
					time.Sleep(100 * time.Millisecond)

				}
			}
			// Expect the test data and a row with a NULL value.
			a.Equalf(expected, count, "mismatch in %s", tables[idx].target)
		})
	}
	sinktest.CheckDiagnostics(ctx, t, loop.Diagnostics)

}

// UTILITIES

// replAdmin provides utilites to manage source and target database.
type replAdmin struct {
	source       *sql.DB
	sourceSchema ident.Schema
	target       *sql.DB
	targetSchema ident.Schema
	restarted    bool
}

type tableMapping struct {
	source    ident.Table
	sourceCdc ident.Table
	target    ident.Table
}

// cdcCmd issues a command to the CDC service on the source
// database, waiting for the given string to appear in CDC status, if supplied.
func (r *replAdmin) cdcCmd(ctx context.Context, command string, expect string) (bool, error) {
	query := fmt.Sprintf("VALUES ASNCDC.ASNCDCSERVICES('%s','asncdc') ", command)
	out, err := r.source.QueryContext(ctx, query)
	if err != nil {
		return false, err
	}
	defer out.Close()
	for out.Next() {
		var msg string
		err := out.Scan(&msg)
		if err != nil {
			return false, err
		}
		done := strings.Contains(msg, expect)
		if done {
			return true, nil
		}
	}
	return false, nil
}

// cdcWaitFor waits until the given string is seen in the CDC status.
func (r *replAdmin) cdcWaitFor(ctx context.Context, expect string) error {
	done := false
	var err error
	for !done {
		done, err = r.cdcCmd(ctx, "status", expect)
		if err != nil {
			return err
		}
		if !done {
			time.Sleep(1000 * time.Millisecond)
		}
	}
	return nil
}

// cdcRestart restarts CDC in the source database
func (r *replAdmin) cdcRestart(ctx context.Context) error {
	err := r.cdcStop(ctx)
	if err != nil {
		return err
	}
	return r.cdcStart(ctx)
}

// cdcStart starts CDC in the source database
func (r *replAdmin) cdcStart(ctx context.Context) error {
	_, err := r.cdcCmd(ctx, "start", "")
	if err != nil {
		return err
	}
	r.restarted = true
	return r.cdcWaitFor(ctx, "is doing work")
}

// cdcReinit re-initializes CDC in the source database
func (r *replAdmin) cdcReinit(ctx context.Context) error {
	up, err := r.cdcCmd(ctx, "status", "is doing work")
	if err != nil {
		return err
	}
	// We try to re-init first, since it's faster.
	// If that fails, we restart it.
	if up {
		reinit, err := r.cdcCmd(ctx, "reinit", "REINIT")
		if err != nil {
			return err
		}
		if reinit {
			log.Info("CDC was already up, we re-init it")
			return r.cdcWaitFor(ctx, "is doing work")
		}
	}
	return r.cdcRestart(ctx)
}

// cdcStop stops CDC in the source database
func (r *replAdmin) cdcStop(ctx context.Context) error {
	_, err := r.cdcCmd(ctx, "stop", "")
	if err != nil {
		return err
	}
	return r.cdcWaitFor(ctx, "asncap is not running")
}

func (r *replAdmin) cleanUp(ctx context.Context) error {
	// if we restarted CDC, we need it to stop it.
	if r.restarted {
		err := r.cdcStop(ctx)
		if err != nil {
			return err
		}
	}
	return r.source.Close()
}

// createRepl sets up the replication environment.
// It creates a schema in the DB2 source.
func createRepl(
	ctx *stopper.Context, fixture *base.Fixture, fc *fixtureConfig, tblName ident.Ident,
) (*replAdmin, *Config, error) {

	dbName, _ := fixture.TargetSchema.Schema().Split()
	// For now, we are using Debezium stored procedure to add tables to CDC.
	// The stored procedure does not deal with case sensitivity well.
	// TODO (silvano): fix stored procedure.
	sourceSchema := ident.MustSchema(ident.New(
		strings.ToUpper(strings.ReplaceAll(dbName.Raw(), "-", "_"))))

	var tbl ident.Table
	if !tblName.Empty() {
		tbl = ident.NewTable(fixture.TargetSchema.Schema(), tblName)
	}
	config, err := getConfig(fixture, fc, tbl,
		sourceSchema, fixture.TargetSchema.Schema())
	if err != nil {
		return nil, nil, err
	}
	conn := &conn{
		columns:     &ident.TableMap[[]types.ColData]{},
		primaryKeys: &ident.TableMap[map[int]int]{},
		config:      config,
	}
	db, err := conn.Open()
	if err != nil {
		return nil, nil, err
	}
	// Create a schema on the source to store all the tables.
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA %s", sourceSchema))
	if err != nil {
		return nil, nil, err
	}
	// To speed up testing, we reduced commit/sleep/monitor interval to decrease latency.
	// With this settings the Capture program has a significant impact on foreground
	// activities, and they are NOT recommended in a production env.
	// COMMIT_INTERVAL = interval in seconds for how often the Capture log reader thread commits.
	// SLEEP_INTERVAL = interval in seconds that the Capture program sleeps when idle.
	// MONITOR_INTERVAL = how often, in seconds, that the monitor thread adds a row to the Capture monitor.
	_, err = db.ExecContext(ctx, "UPDATE ASNCDC.IBMSNAP_CAPPARMS SET COMMIT_INTERVAL=2,SLEEP_INTERVAL=1,MONITOR_INTERVAL=5")
	if err != nil {
		return nil, nil, err
	}
	// We start the replication from the current max log sequence number
	lsn, err := getCurrentLsn(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	config.DefaultConsistentPoint = string(lsn.Value)
	return &replAdmin{
		source:       db,
		sourceSchema: sourceSchema,
		target:       fixture.TargetPool.DB,
		targetSchema: fixture.TargetSchema.Schema(),
	}, config, nil
}

func (r *replAdmin) createTables(
	ctx context.Context, table tableInfo,
) (*tableMapping, func() error, error) {
	// TODO (silvano): fix case sensitivity.
	tb := ident.New(strings.ToUpper(table.name.Raw()))
	tgt := ident.NewTable(r.targetSchema, tb)
	src := ident.NewTable(r.sourceSchema, tb)
	cdc := ident.NewTable(cdcSchema, ident.New(strings.ToUpper(fmt.Sprintf("CDC_%s_%s",
		r.sourceSchema.Raw(), tb.Raw()))))
	res := &tableMapping{
		source:    src,
		sourceCdc: cdc,
		target:    tgt,
	}
	// Create the schema in both locations.
	_, err := r.source.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s ("PK" INT PRIMARY KEY not null, %s %s)`,
		src, table.sourceColName, table.sourceColType))
	if err != nil {
		return &tableMapping{}, nil, err
	}
	_, err = r.target.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s ("PK" INT PRIMARY KEY, %s %s)`,
		tgt, table.targetColName, table.targetColType))
	if err != nil {
		return &tableMapping{}, nil, err
	}
	// enable CDC for the table
	_, err = r.source.ExecContext(ctx, fmt.Sprintf("CALL ASNCDC.ADDTABLE('%s','%s'); ", src.Schema().Raw(), src.Table().Raw()))
	if err != nil {
		return &tableMapping{}, nil, err
	}
	return res, func() error {
		query := fmt.Sprintf("CALL ASNCDC.REMOVETABLE('%s','%s'); ", src.Schema().Raw(), src.Table().Raw())
		_, err := r.source.ExecContext(ctx, query)
		return err
	}, nil
}

// getConfig is an helper function to create a configuration for the connector
func getConfig(
	fixture *base.Fixture,
	fc *fixtureConfig,
	tbl ident.Table,
	dbName ident.Schema,
	targetSchema ident.Schema,
) (*Config, error) {

	conn := defaultSourceConn
	if found := os.Getenv("TEST_DB2_CONN"); len(found) > 0 {
		conn = found
	}
	log.Infof("Starting source table: %s %s %v", conn, tbl, fc)
	config := &Config{
		BaseConfig: logical.BaseConfig{
			ApplyTimeout:  5 * time.Minute, // Increase to make using the debugger easier.
			ChaosProb:     fc.chaosProb,
			Immediate:     fc.immediate,
			RetryDelay:    100 * time.Millisecond,
			StagingSchema: fixture.StagingDB.Schema(),
			TargetConn:    fixture.TargetPool.ConnectionString,
		},
		LoopConfig: logical.LoopConfig{
			LoopName:     "db2",
			TargetSchema: targetSchema,
		},
		SourceConn:   conn,
		SourceSchema: dbName,
	}
	if fc.backfill {
		config.BackfillWindow = time.Minute
	}
	if fc.script {
		config.ScriptConfig = script.Config{
			FS:       scripttest.ScriptFSFor(tbl),
			MainPath: "/testdata/logical_test_db2.ts",
		}
	}
	return config, config.Preflight()
}

// getCurrentLsn retrieves the latest sequence number in the monitoring tables.
func getCurrentLsn(ctx *stopper.Context, db *sql.DB) (*lsn, error) {
	rows, err := db.QueryContext(ctx, "select MAX(RESTART_SEQ) from ASNCDC.IBMSNAP_CAPMON")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var v []byte
		err = rows.Scan(&v)
		if err == nil {
			if len(v) == 0 {
				return lsnZero(), nil
			}
			return &lsn{
				Value: v,
			}, err
		}
		return nil, err
	}
	return lsnZero(), nil
}

func (r *replAdmin) insertValues(
	ctx context.Context, tbl ident.Table, values []any, insertNull bool,
) error {
	tx, err := r.source.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for valIdx, value := range values {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s VALUES (?, ?)`, tbl), valIdx, value); err != nil {
			return err
		}
	}
	if insertNull {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s VALUES (-1, NULL)`, tbl)); err != nil {
			return err
		}
	}
	return tx.Commit()
}
