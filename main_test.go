// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

const endpointTest = "test.sql"

// getDB creates a new testing DB, return the name of that db and a closer that
// will drop the table and close the db connection.
func getDB(ctx context.Context) (db *pgxpool.Pool, dbName string, closer func(), err error) {
	db, err = pgxpool.Connect(ctx, *connectionString)
	if err != nil {
		return
	}

	// Create the testing database
	dbNum := r.Intn(10000)
	dbName = fmt.Sprintf("_test_db_%d", dbNum)

	if err = Execute(ctx, db, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)); err != nil {
		return
	}

	if err = Execute(ctx, db, fmt.Sprintf(sinkDBZoneConfig, dbName)); err != nil {
		return
	}

	if err = Execute(ctx, db, "SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		return
	}

	closer = func() {
		if err = Execute(ctx, db, fmt.Sprintf("DROP DATABASE %s CASCADE", dbName)); err != nil {
			return
		}
		db.Close()
	}

	return
}

func getRowCount(ctx context.Context, db *pgxpool.Pool, fullTableName string) (int, error) {
	var count int
	if err := Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", fullTableName)).Scan(&count)
	}); err != nil {
		return 0, err
	}
	return count, nil
}

type tableInfo struct {
	db     *pgxpool.Pool
	dbName string
	name   string
}

func (ti tableInfo) String() string {
	return fmt.Sprintf("%s.%s", ti.dbName, ti.name)
}

func (ti tableInfo) getFullName() string {
	return fmt.Sprintf("%s.%s", ti.dbName, ti.name)
}

func (ti *tableInfo) deleteAll(ctx context.Context) error {
	return Execute(ctx, ti.db, fmt.Sprintf("DELETE FROM %s WHERE true", ti.getFullName()))
}

func (ti tableInfo) getTableRowCount(ctx context.Context) (int, error) {
	return getRowCount(ctx, ti.db, ti.getFullName())
}

func (ti tableInfo) dropTable(ctx context.Context) error {
	return Execute(ctx, ti.db, fmt.Sprintf("DROP TABLE IF EXISTS %s", ti.getFullName()))
}

// This function creates a test table and returns a unique name.
// The schemaSpec parameter must have exactly two %s substitution
// parameters for the database name and table name.
func createTestTable(ctx context.Context, db *pgxpool.Pool, dbName, schemaSpec string) (tableInfo, error) {
	var tableName string

outer:
	for {
		// Create the testing database
		tableNum := r.Intn(10000)
		tableName = fmt.Sprintf("_test_table_%d", tableNum)

		// Find the DB.
		var actualTableName string
		err := Retry(ctx, func(ctx context.Context) error {
			return db.QueryRow(ctx,
				fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = $1", dbName),
				tableName,
			).Scan(&actualTableName)
		})
		switch err {
		case pgx.ErrNoRows:
			break outer
		case nil:
			continue
		default:
			return tableInfo{}, err
		}
	}

	if err := Execute(ctx, db, fmt.Sprintf(schemaSpec, dbName, tableName)); err != nil {
		return tableInfo{}, err
	}

	return tableInfo{
		db:     db,
		dbName: dbName,
		name:   tableName,
	}, nil
}

type tableInfoSimple struct {
	tableInfo
	rowCount int
}

const tableSimpleSchema = `
CREATE TABLE %s.%s (
	a INT PRIMARY KEY,
	b INT
)
`

func createTestSimpleTable(ctx context.Context, db *pgxpool.Pool, dbName string) (tableInfoSimple, error) {
	info, err := createTestTable(ctx, db, dbName, tableSimpleSchema)
	return tableInfoSimple{tableInfo: info}, err
}

func (tis *tableInfoSimple) populateTable(ctx context.Context, count int) error {
	for i := 0; i < count; i++ {
		if err := Execute(
			ctx,
			tis.db,
			fmt.Sprintf("INSERT INTO %s VALUES ($1, $1)", tis.getFullName()),
			tis.rowCount+1,
		); err != nil {
			return err
		}
		tis.rowCount++
	}
	return nil
}

func (tis *tableInfoSimple) updateNoneKeyColumns(ctx context.Context) error {
	return Execute(
		ctx,
		tis.db,
		fmt.Sprintf("UPDATE %s SET b=b*100 WHERE true", tis.getFullName()),
	)
}

func (tis *tableInfoSimple) updateAll(ctx context.Context) error {
	return Execute(
		ctx,
		tis.db,
		fmt.Sprintf("UPDATE %s SET a=a*100000, b=b*100000 WHERE true", tis.getFullName()),
	)
}

func (tis *tableInfoSimple) maxB(ctx context.Context) (int, error) {
	var max int
	err := Retry(ctx, func(ctx context.Context) error {
		return tis.db.QueryRow(
			ctx,
			fmt.Sprintf("SELECT max(b) FROM %s", tis.getFullName()),
		).Scan(&max)
	})
	return max, err
}

// tableInfoComposite is a table with a composite primary key.
type tableInfoComposite struct {
	tableInfo
	rowCount int
}

const tableCompositeSchema = `
CREATE TABLE %s.%s (
	a INT,
	b INT,
	c INT,
	PRIMARY KEY (a, b)
)
`

func createTestCompositeTable(ctx context.Context, db *pgxpool.Pool, dbName string) (tableInfoSimple, error) {
	info, err := createTestTable(ctx, db, dbName, tableCompositeSchema)
	return tableInfoSimple{tableInfo: info}, err
}

func (tis *tableInfoComposite) populateTable(ctx context.Context, count int) error {
	for i := 0; i < count; i++ {
		if err := Execute(
			ctx,
			tis.db,
			fmt.Sprintf("INSERT INTO %s VALUES ($1, $1, $1)", tis.getFullName()),
			tis.rowCount+1,
		); err != nil {
			return err
		}
		tis.rowCount++
	}
	return nil
}

type tableInfoClob struct {
	tableInfo
	clobSize int // The number of bytes to generate per row.
	rowCount int // A running total for code generation.
}

const tableClobSchema = `
CREATE TABLE %s.%s (
  a INT NOT NULL PRIMARY KEY,
  data TEXT
)
`

func createTestClobTable(ctx context.Context, db *pgxpool.Pool, dbName string, clobSize int) (tableInfoClob, error) {
	if clobSize <= 0 {
		clobSize = 8 * 1024
	}
	info, err := createTestTable(ctx, db, dbName, tableClobSchema)
	return tableInfoClob{tableInfo: info, clobSize: clobSize}, err
}

func (tic *tableInfoClob) populateTable(ctx context.Context, count int) error {
	for i := 0; i < count; i++ {
		c := tic.rowCount + 1
		data, err := ioutil.ReadAll(clobData(tic.clobSize, c))
		if err != nil {
			return err
		}
		if err := Execute(
			ctx,
			tic.db,
			fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tic.getFullName()),
			c,
			string(data),
		); err != nil {
			return err
		}
		tic.rowCount++
	}
	return nil
}

type jobInfo struct {
	db *pgxpool.Pool
	id int
}

func (ji *jobInfo) cancelJob(ctx context.Context) error {
	if ji.id == 0 {
		return nil
	}
	if err := Execute(ctx, ji.db, fmt.Sprintf("CANCEL JOB %d", ji.id)); err != nil {
		return err
	}
	ji.id = 0
	return nil
}

func createChangeFeed(
	ctx context.Context, db *pgxpool.Pool, url string, endpoint string, tis ...tableInfo,
) (jobInfo, error) {
	var query strings.Builder
	fmt.Fprint(&query, "CREATE CHANGEFEED FOR TABLE ")
	for i := 0; i < len(tis); i++ {
		if i != 0 {
			fmt.Fprint(&query, ", ")
		}
		fmt.Fprintf(&query, tis[i].getFullName())
	}
	fmt.Fprintf(&query, " INTO 'experimental-%s/%s' WITH updated,resolved", url, endpoint)
	var jobID int
	err := Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(ctx, query.String()).Scan(&jobID)
	})
	return jobInfo{
		db: db,
		id: jobID,
	}, err
}

// dropSinkDB is just a wrapper around DropSinkDB for testing.
func dropSinkDB(ctx context.Context, db *pgxpool.Pool) error {
	return DropSinkDB(ctx, db)
}

// createSinkDB will first drop then create a new sink db.
func createSinkDB(ctx context.Context, db *pgxpool.Pool) error {
	if err := dropSinkDB(ctx, db); err != nil {
		return err
	}
	return CreateSinkDB(ctx, db)
}

// TestDB is just a quick test to create and drop a database to ensure the
// Cockroach Cluster is working correctly and we have the correct permissions.
func TestDB(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Find the DB.
	var actualDBName string
	if err := Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(
			ctx,
			`SELECT database_name FROM [SHOW DATABASES] WHERE database_name = $1`, dbName,
		).Scan(&actualDBName)
	}); !a.NoError(err) {
		return
	}

	if !a.Equal(actualDBName, dbName, "db names do not match") {
		return
	}

	// Create a test table and insert some rows
	table, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}
	if !a.NoError(table.populateTable(ctx, 10)) {
		return
	}
	count, err := table.getTableRowCount(ctx)
	a.Equal(10, count, "row count")
	a.NoError(err)
}

func createConfig(source tableInfo, destination tableInfo, endpoint string) Config {
	return Config{
		ConfigEntry{
			Endpoint:            endpoint,
			SourceTable:         source.name,
			DestinationDatabase: destination.dbName,
			DestinationTable:    destination.name,
		},
	}
}

func TestFeedInsert(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Give the from table a few rows
	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	count, err := tableFrom.getTableRowCount(ctx)
	a.Equal(10, count, "rows")
	if !a.NoError(err) {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job, err := createChangeFeed(ctx, db, server.URL, endpointTest, tableFrom.tableInfo)
	if !a.NoError(err) {
		return
	}
	defer job.cancelJob(ctx)

	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}

	// Wait for sync to occur.
	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}
	for {
		if !a.NoError(ctx.Err()) {
			return
		}
		toCount, err := tableTo.getTableRowCount(ctx)
		if !a.NoError(err) {
			return
		}
		fromCount, err := tableFrom.getTableRowCount(ctx)
		if !a.NoError(err) {
			return
		}
		if toCount == fromCount {
			break
		}
	}

	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}

	// Wait for sync to occur again.
	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	sinkCount, err := getRowCount(ctx, db, sink.sinkTableFullName)
	a.Equal(0, sinkCount, "sink table not empty")
	a.NoError(err)
}

func TestFeedDelete(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Give the from table a few rows
	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	if count, err := tableFrom.getTableRowCount(ctx); !a.Equal(10, count, "row count") || !a.NoError(err) {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job, err := createChangeFeed(ctx, db, server.URL, endpointTest, tableFrom.tableInfo)
	if !a.NoError(err) {
		return
	}
	defer job.cancelJob(ctx)

	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}

	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	if !a.NoError(tableFrom.deleteAll(ctx)) {
		return
	}

	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	sinkCount, err := getRowCount(ctx, db, sink.sinkTableFullName)
	a.Equal(0, sinkCount, "expected empty sink table")
	a.NoError(err)
}

func TestFeedDeleteCompositeKey(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestCompositeTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestCompositeTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Give the from table a few rows
	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	if count, err := tableFrom.getTableRowCount(ctx); !a.Equal(10, count, "row count") || !a.NoError(err) {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()
	t.Log(server.URL)

	job, err := createChangeFeed(ctx, db, server.URL, endpointTest, tableFrom.tableInfo)
	if !a.NoError(err) {
		return
	}
	defer job.cancelJob(ctx)

	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}

	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	if !a.NoError(tableFrom.deleteAll(ctx)) {
		return
	}

	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	sinkCount, err := getRowCount(ctx, db, sink.sinkTableFullName)
	a.Equal(0, sinkCount, "expected empty sink table")
	a.NoError(err)
}

func TestFeedUpdateNonPrimary(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Give the from table a few rows
	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	if count, err := tableFrom.getTableRowCount(ctx); !a.Equal(10, count) || !a.NoError(err) {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job, err := createChangeFeed(ctx, db, server.URL, endpointTest, tableFrom.tableInfo)
	if !a.NoError(err) {
		return
	}
	defer job.cancelJob(ctx)

	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}

	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	if !a.NoError(tableFrom.updateNoneKeyColumns(ctx)) {
		return
	}

	if !a.NoError(loopUntilMaxB(ctx, &tableFrom, &tableTo)) {
		return
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	sinkCount, err := getRowCount(ctx, db, sink.sinkTableFullName)
	a.Equal(0, sinkCount, "expected empty sink table")
	a.NoError(err)
}

func TestFeedUpdatePrimary(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestSimpleTable(ctx, db, dbName)
	if !a.NoError(err) {
		return
	}

	// Give the from table a few rows
	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	if count, err := tableFrom.getTableRowCount(ctx); !a.Equal(10, count, "row count") || !a.NoError(err) {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job, err := createChangeFeed(ctx, db, server.URL, endpointTest, tableFrom.tableInfo)
	defer job.cancelJob(ctx)

	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}

	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	if !a.NoError(tableFrom.updateAll(ctx)) {
		return
	}

	if !a.NoError(loopUntilMaxB(ctx, &tableFrom, &tableTo)) {
		return
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	sinkCount, err := getRowCount(ctx, db, sink.sinkTableFullName)
	a.Equal(0, sinkCount, "expected empty sink table")
	a.NoError(err)
}

func TestTypes(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the sinks
	sinks, err := CreateSinks(ctx, db, []ConfigEntry{})
	if !a.NoError(err) {
		return
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

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
		//		{`geography`, `GEOGRAPHY`, `0101000020E6100000000000000000F03F0000000000000040`, false},
		//		{`geometry`, `GEOMETRY`, `010100000075029A081B9A5DC0F085C954C1F84040`, false},
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

	tableIndexableSchema := `CREATE TABLE %s (a %s PRIMARY KEY,	b %s)`
	tableNonIndexableSchema := `CREATE TABLE %s (a INT PRIMARY KEY,	b %s)`

	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)
			ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()

			tableIn := tableInfo{
				db:     db,
				dbName: dbName,
				name:   fmt.Sprintf("in_%s", test.name),
			}
			tableOut := tableInfo{
				db:     db,
				dbName: dbName,
				name:   fmt.Sprintf("out_%s", test.name),
			}

			// Drop both tables if they already exist.
			if !a.NoError(tableIn.dropTable(ctx)) {
				return
			}
			if !a.NoError(tableOut.dropTable(ctx)) {
				return
			}

			// Create both tables.
			if test.indexable {
				if !a.NoError(Execute(ctx, db, fmt.Sprintf(
					tableIndexableSchema, tableIn.getFullName(), test.columnType, test.columnType,
				))) {
					return
				}
				if !a.NoError(Execute(ctx, db, fmt.Sprintf(
					tableIndexableSchema, tableOut.getFullName(), test.columnType, test.columnType,
				))) {
					return
				}
			} else {
				if !a.NoError(Execute(ctx, db, fmt.Sprintf(
					tableNonIndexableSchema, tableIn.getFullName(), test.columnType,
				))) {
					return
				}
				if !a.NoError(Execute(ctx, db, fmt.Sprintf(
					tableNonIndexableSchema, tableOut.getFullName(), test.columnType,
				))) {
					return
				}
			}

			// Defer a table drop for both tables to clean them up.
			defer tableIn.dropTable(ctx)
			defer tableOut.dropTable(ctx)

			// Create the sink
			// There is no way to remove a sink at this time, and that should be ok
			// for these tests.
			if !a.NoError(sinks.AddSink(ctx, db, ConfigEntry{
				Endpoint:            endpointTest,
				DestinationDatabase: dbName,
				DestinationTable:    tableOut.name,
				SourceTable:         tableIn.name,
			})) {
				return
			}

			// Create the CDC feed.
			job, err := createChangeFeed(ctx, db, server.URL, endpointTest, tableIn)
			if !a.NoError(err) {
				return
			}
			defer job.cancelJob(ctx)

			// Insert a row into the in table.
			if test.indexable {
				if !a.NoError(Execute(ctx, db,
					fmt.Sprintf("INSERT INTO %s (a,b) VALUES ($1,$2)", tableIn.getFullName()),
					test.columnValue, test.columnValue,
				)) {
					return
				}
			} else {
				value := interface{}(test.columnValue)
				if len(test.columnValue) == 0 {
					value = nil
				}
				if !a.NoError(Execute(ctx, db,
					fmt.Sprintf("INSERT INTO %s (a, b) VALUES (1, $1)", tableIn.getFullName()),
					value,
				)) {
					return
				}
			}

			// Wait until the out table has a row.
			for {
				count, err := tableOut.getTableRowCount(ctx)
				if !a.NoError(err) {
					return
				}
				if count > 0 {
					break
				}
			}

			// Now fetch that rows and compare them.
			var inA, inB interface{}
			if !a.NoError(Retry(ctx, func(ctx context.Context) error {
				return db.QueryRow(ctx,
					fmt.Sprintf("SELECT a, b FROM %s LIMIT 1", tableIn.getFullName()),
				).Scan(&inA, &inB)
			})) {
				return
			}
			var outA, outB interface{}
			if !a.NoError(Retry(ctx, func(ctx context.Context) error {
				return db.QueryRow(ctx,
					fmt.Sprintf("SELECT a, b FROM %s LIMIT 1", tableOut.getFullName()),
				).Scan(&outA, &outB)
			})) {
				return
			}
			a.Equal(fmt.Sprintf("%v", inA), fmt.Sprintf("%v", outA), "A")
			a.Equal(fmt.Sprintf("%v", inB), fmt.Sprintf("%v", outB), "B")
		})
	}
}

func TestConfig(t *testing.T) {
	testCases := []struct {
		name           string
		testJSON       string
		expectedPass   bool
		expectedConfig Config
	}{
		{
			name:         "empty",
			testJSON:     "",
			expectedPass: false,
		},
		{
			name:         "empty2",
			testJSON:     "[]",
			expectedPass: false,
		},
		{
			name:         "empty3",
			testJSON:     "[{}]",
			expectedPass: false,
		},
		{
			name:         "missing endpoint",
			testJSON:     `[{"source_table":"s_tbl", "destination_database":"d_db", "destination_table":"dt_tbl"}]`,
			expectedPass: false,
		},
		{
			name:         "missing source table",
			testJSON:     `[{"endpoint":"test.sql", "destination_database":"d_db", "destination_table":"dt_tbl"}]`,
			expectedPass: false,
		},
		{
			name:         "missing destination database",
			testJSON:     `[{"endpoint":"test.sql", "source_table":"s_tbl", "destination_table":"dt_tbl"}]`,
			expectedPass: false,
		},
		{
			name:         "missing destination table",
			testJSON:     `[{"endpoint":"test.sql", "source_table":"s_tbl", "destination_database":"d_db"}]`,
			expectedPass: false,
		},
		{
			name:         "empty endpoint",
			testJSON:     `[{"endpoint":"", "source_table":"s_tbl", "destination_database":"d_db", "destination_table":"dt_tbl"}]`,
			expectedPass: false,
		},
		{
			name:         "empty source table",
			testJSON:     `[{"endpoint":"test.sql", "source_table":"", "destination_database":"d_db", "destination_table":"dt_tbl"}]`,
			expectedPass: false,
		},
		{
			name:         "empty destination database",
			testJSON:     `[{"endpoint":"test.sql", "source_table":"s_tbl", "destination_database":"", "destination_table":"dt_tbl"}]`,
			expectedPass: false,
		},
		{
			name:         "empty destination table",
			testJSON:     `[{"endpoint":"test.sql", "source_table":"s_tbl", "destination_database":"d_db", "destination_table":""}]`,
			expectedPass: false,
		},
		{
			name:         "single",
			testJSON:     `[{"endpoint":"test.sql", "source_table":"s_tbl", "destination_database":"d_db", "destination_table":"d_tbl"}]`,
			expectedPass: true,
			expectedConfig: Config{
				ConfigEntry{Endpoint: "test.sql", SourceTable: "s_tbl", DestinationDatabase: "d_db", DestinationTable: "d_tbl"},
			},
		},
		{
			name: "double",
			testJSON: `[
	{"endpoint":"test.sql", "source_table":"s_tbl1", "destination_database":"d_db", "destination_table":"d_tbl1"},
	{"endpoint":"test.sql", "source_table":"s_tbl2", "destination_database":"d_db", "destination_table":"d_tbl2"}
]`,
			expectedPass: true,
			expectedConfig: Config{
				ConfigEntry{Endpoint: "test.sql", SourceTable: "s_tbl1", DestinationDatabase: "d_db", DestinationTable: "d_tbl1"},
				ConfigEntry{Endpoint: "test.sql", SourceTable: "s_tbl2", DestinationDatabase: "d_db", DestinationTable: "d_tbl2"},
			},
		},
		{
			name: "triple",
			testJSON: `[
	{"endpoint":"test1.sql", "source_table":"s_tbl1", "destination_database":"d_db1", "destination_table":"d_tbl1"},
	{"endpoint":"test1.sql", "source_table":"s_tbl2", "destination_database":"d_db1", "destination_table":"d_tbl2"},
	{"endpoint":"test2.sql", "source_table":"s_tbl3", "destination_database":"d_db2", "destination_table":"d_tbl3"}
]`,
			expectedPass: true,
			expectedConfig: Config{
				ConfigEntry{Endpoint: "test1.sql", SourceTable: "s_tbl1", DestinationDatabase: "d_db1", DestinationTable: "d_tbl1"},
				ConfigEntry{Endpoint: "test1.sql", SourceTable: "s_tbl2", DestinationDatabase: "d_db1", DestinationTable: "d_tbl2"},
				ConfigEntry{Endpoint: "test2.sql", SourceTable: "s_tbl3", DestinationDatabase: "d_db2", DestinationTable: "d_tbl3"},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)

			actual, err := parseConfig(test.testJSON)
			if test.expectedPass {
				a.NoError(err)
				a.True(reflect.DeepEqual(test.expectedConfig, actual))
			} else {
				a.Error(err)
			}
		})
	}
}

func TestMultipleFeeds(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	testcases := []struct {
		feedCount     int
		tablesPerFeed int
		populateCount int
	}{
		{1, 1, 1000},
		{1, 2, 10},
		{1, 3, 10},
		{2, 1, 10},
		{2, 2, 10},
		{2, 3, 10},
		{3, 1, 10},
		{3, 2, 10},
		{3, 3, 10},
	}

	nameEndpoint := func(feedID int) string {
		return fmt.Sprintf("test_%d_%s", feedID, endpointTest)
	}

	for _, testcase := range testcases {
		t.Run(fmt.Sprintf("Feeds_%d_Tables_%d_Size_%d",
			testcase.feedCount, testcase.tablesPerFeed, testcase.populateCount,
		), func(t *testing.T) {
			a := assert.New(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// Create a new _cdc_sink db
			if !a.NoError(createSinkDB(ctx, db)) {
				return
			}
			defer dropSinkDB(ctx, db)

			// Create all the tables
			var sourceTablesByFeed [][]*tableInfoSimple
			var destinationTablesByFeed [][]*tableInfoSimple
			for i := 0; i < testcase.feedCount; i++ {
				var sourceTables []*tableInfoSimple
				var destinationTables []*tableInfoSimple
				for j := 0; j < testcase.tablesPerFeed; j++ {
					sourceTable, err := createTestSimpleTable(ctx, db, dbName)
					if !a.NoErrorf(err, "create source i=%d, j=%d", i, j) {
						return
					}
					sourceTables = append(sourceTables, &sourceTable)
					destinationTable, err := createTestSimpleTable(ctx, db, dbName)
					if !a.NoErrorf(err, "create dest i=%d, j=%d", i, j) {
						return
					}
					destinationTables = append(destinationTables, &destinationTable)
				}
				sourceTablesByFeed = append(sourceTablesByFeed, sourceTables)
				destinationTablesByFeed = append(destinationTablesByFeed, destinationTables)
			}

			// Populate all the source tables
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					if !a.NoError(table.populateTable(ctx, testcase.populateCount), table.name) {
						return
					}
				}
			}

			// Create the sinks
			sinks, err := CreateSinks(ctx, db, []ConfigEntry{})
			if !a.NoError(err) {
				return
			}

			// Create all the sinks
			for i := 0; i < testcase.feedCount; i++ {
				for j := 0; j < testcase.tablesPerFeed; j++ {
					if !a.NoErrorf(sinks.AddSink(ctx, db, ConfigEntry{
						Endpoint:            nameEndpoint(i),
						DestinationDatabase: destinationTablesByFeed[i][j].dbName,
						DestinationTable:    destinationTablesByFeed[i][j].name,
						SourceTable:         sourceTablesByFeed[i][j].name,
					}), "AddSink i=%d j=%d", i, j) {
						return
					}
				}
			}

			// Create a test http server
			handler := createHandler(db, sinks)
			server := httptest.NewServer(http.HandlerFunc(handler))
			defer server.Close()

			// Create the changefeeds
			for i := 0; i < testcase.feedCount; i++ {
				var tableInfos []tableInfo
				for _, table := range sourceTablesByFeed[i] {
					tableInfos = append(tableInfos, table.tableInfo)
				}
				job, err := createChangeFeed(ctx, db, server.URL, nameEndpoint(i), tableInfos...)
				if !a.NoErrorf(err, "changefeed %d", i) {
					return
				}
				defer job.cancelJob(ctx)
			}

			// Add some more lines to each table.
			// Populate all the source tables
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					if !a.NoError(table.populateTable(ctx, testcase.populateCount), table.name) {
						return
					}
				}
			}

			// Make sure each table has 20 rows
			for _, feedTables := range destinationTablesByFeed {
				for _, table := range feedTables {
					// Wait until table is populated
					for {
						count, err := table.getTableRowCount(ctx)
						if !a.NoError(err, table) {
							return
						}
						if count == testcase.populateCount*2 {
							break
						}
					}
				}
			}

			// Update all rows in the source table.
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					a.NoErrorf(table.updateAll(ctx), "updateAll %s", table)
				}
			}

			// Make sure each table has 20 rows
			for i, feedTables := range destinationTablesByFeed {
				for j, table := range feedTables {
					tableB, err := table.maxB(ctx)
					if !a.NoError(err, table.String()) {
						return
					}
					sourceB, err := sourceTablesByFeed[i][j].maxB(ctx)
					if !a.NoError(err, sourceTablesByFeed[i][j].String()) {
						return
					}
					if tableB == sourceB {
						break
					}
				}
			}

			// Delete all rows in the table.
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					a.NoErrorf(table.deleteAll(ctx), "deleting %s", table)
				}
			}

			// Make sure each table is drained.
			for _, feedTables := range destinationTablesByFeed {
				for _, table := range feedTables {
					for {
						count, err := table.getTableRowCount(ctx)
						if !a.NoError(err) {
							return
						}
						if count == 0 {
							break
						}
					}
				}
			}
		})
	}
}

func TestLargeClobs(t *testing.T) {
	const clobSize = 5 * 1024
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the test db
	db, dbName, dbClose, err := getDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer dbClose()

	// Create a new _cdc_sink db
	if !a.NoError(createSinkDB(ctx, db)) {
		return
	}
	defer dropSinkDB(ctx, db)

	// Create the table to import from
	tableFrom, err := createTestClobTable(ctx, db, dbName, clobSize)
	if !a.NoError(err) {
		return
	}

	// Create the table to receive into
	tableTo, err := createTestClobTable(ctx, db, dbName, clobSize)
	if !a.NoError(err) {
		return
	}

	// Give the from table a few rows
	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	if count, err := tableFrom.getTableRowCount(ctx); !a.Equal(10, count, "row count") || !a.NoError(err) {
		return
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(ctx, db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if !a.NoError(err) {
		return
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job, err := createChangeFeed(ctx, db, server.URL, endpointTest, tableFrom.tableInfo)
	defer job.cancelJob(ctx)

	if !a.NoError(tableFrom.populateTable(ctx, 10)) {
		return
	}
	t.Log("Waiting for sync")
	if !a.NoError(loopUntilSync(ctx, tableFrom, tableTo)) {
		return
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	sinkCount, err := getRowCount(ctx, db, sink.sinkTableFullName)
	a.Equal(0, sinkCount, "expected empty sink table")
	a.NoError(err)
}

func loopUntilMaxB(
	ctx context.Context,
	tableTo, tableFrom interface {
		maxB(context.Context) (int, error)
	},
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		toCount, err := tableTo.maxB(ctx)
		if err != nil {
			return errors.Wrap(err, "querying to")
		}
		fromCount, err := tableFrom.maxB(ctx)
		if err != nil {
			return errors.Wrap(err, "querying from")
		}
		if toCount == fromCount {
			break
		}
	}
	return nil
}

func loopUntilSync(
	ctx context.Context,
	tableTo, tableFrom interface {
		getTableRowCount(context.Context) (int, error)
	},
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		toCount, err := tableTo.getTableRowCount(ctx)
		if err != nil {
			return errors.Wrap(err, "querying to")
		}
		fromCount, err := tableFrom.getTableRowCount(ctx)
		if err != nil {
			return errors.Wrap(err, "querying from")
		}
		if toCount == fromCount {
			break
		}
	}
	return nil
}

// clobData returns a reader that will generate some number of bytes.
// The nonce value is used to perturb the sequence.
func clobData(legnth, nonce int) io.Reader {
	ret := &io.LimitedReader{R: &clobSourceReader{}, N: int64(nonce + legnth)}
	nonce = nonce % len(clobSourceTest)
	_, _ = io.CopyN(io.Discard, ret, int64(nonce))
	return ret
}

const clobSourceTest = "_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ!"

// clobSourceReader returns an infinitely long sequence of data. Use
// the clobData function instead.
type clobSourceReader struct{}

// Read will fill the buffer with data.
func (c *clobSourceReader) Read(p []byte) (n int, err error) {
	ret := len(p)
	for len(p) >= len(clobSourceTest) {
		copy(p, clobSourceTest)
		p = p[len(clobSourceTest):]
	}
	if rem := len(p); rem > 0 {
		copy(p, clobSourceTest[:rem])
	}
	return ret, nil
}
