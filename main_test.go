package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
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
func getDB(t *testing.T) (db *sql.DB, dbName string, closer func()) {
	var err error
	db, err = sql.Open("postgres", *connectionString)
	if err != nil {
		t.Fatal(err)
	}

	// Create the testing database
	dbNum := r.Intn(10000)
	dbName = fmt.Sprintf("_test_db_%d", dbNum)

	t.Logf("Testing Database: %s", dbName)

	if err := Execute(db, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)); err != nil {
		t.Fatal(err)
	}

	if err := Execute(db, fmt.Sprintf(sinkDBZoneConfig, dbName)); err != nil {
		t.Fatal(err)
	}

	if err := Execute(db, "SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		t.Fatal(err)
	}

	closer = func() {
		if err := Execute(db, fmt.Sprintf("DROP DATABASE %s CASCADE", dbName)); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	return
}

func getRowCount(t *testing.T, db *sql.DB, fullTableName string) int {
	var count int
	if err := crdb.Execute(func() error {
		return db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", fullTableName)).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

type tableInfo struct {
	db     *sql.DB
	dbName string
	name   string
}

func (ti tableInfo) getFullName() string {
	return fmt.Sprintf("%s.%s", ti.dbName, ti.name)
}

func (ti *tableInfo) deleteAll(t *testing.T) {
	if err := Execute(ti.db, fmt.Sprintf("DELETE FROM %s WHERE true", ti.getFullName())); err != nil {
		t.Fatal(err)
	}
}

func (ti tableInfo) getTableRowCount(t *testing.T) int {
	return getRowCount(t, ti.db, ti.getFullName())
}

func (ti tableInfo) dropTable(t *testing.T) {
	if err := Execute(ti.db, fmt.Sprintf("DROP TABLE IF EXISTS %s", ti.getFullName())); err != nil {
		t.Fatal(err)
	}
}

// This function creates a test table and returns its name.
func createTestTable(t *testing.T, db *sql.DB, dbName string, schema string) tableInfo {
	var tableName string

outer:
	for {
		// Create the testing database
		tableNum := r.Intn(10000)
		tableName = fmt.Sprintf("_test_table_%d", tableNum)

		// Find the DB.
		var actualTableName string
		err := crdb.Execute(func() error {
			return db.QueryRow(
				fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = $1", dbName),
				tableName,
			).Scan(&actualTableName)
		})
		switch err {
		case sql.ErrNoRows:
			break outer
		case nil:
			continue
		default:
			t.Fatal(err)
		}
	}

	if err := Execute(db, fmt.Sprintf(tableSimpleSchema, dbName, tableName)); err != nil {
		t.Fatal(err)
	}

	t.Logf("Testing Table: %s.%s", dbName, tableName)
	return tableInfo{
		db:     db,
		dbName: dbName,
		name:   tableName,
	}
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

func createTestSimpleTable(t *testing.T, db *sql.DB, dbName string) tableInfoSimple {
	return tableInfoSimple{
		tableInfo: createTestTable(t, db, dbName, tableSimpleSchema),
	}
}

func (tis *tableInfoSimple) populateTable(t *testing.T, count int) {
	for i := 0; i < count; i++ {
		if err := Execute(
			tis.db,
			fmt.Sprintf("INSERT INTO %s VALUES ($1, $1)", tis.getFullName()),
			tis.rowCount+1,
		); err != nil {
			t.Fatal(err)
		}
		tis.rowCount++
	}
}

func (tis *tableInfoSimple) updateNoneKeyColumns(t *testing.T) {
	if err := Execute(
		tis.db,
		fmt.Sprintf("UPDATE %s SET b=b*100 WHERE true", tis.getFullName()),
	); err != nil {
		t.Fatal(err)
	}
}

func (tis *tableInfoSimple) updateAll(t *testing.T) {
	if err := Execute(
		tis.db,
		fmt.Sprintf("UPDATE %s SET a=a*100, b=b*100 WHERE true", tis.getFullName()),
	); err != nil {
		t.Fatal(err)
	}
}

func (tis *tableInfoSimple) maxB(t *testing.T) int {
	var max int
	if err := crdb.Execute(func() error {
		return tis.db.QueryRow(
			fmt.Sprintf("SELECT max(b) FROM %s", tis.getFullName()),
		).Scan(&max)
	}); err != nil {
		t.Fatal(err)
	}
	return max
}

type jobInfo struct {
	db *sql.DB
	id int
}

func (ji *jobInfo) cancelJob(t *testing.T) {
	if ji.id == 0 {
		return
	}
	if err := Execute(ji.db, fmt.Sprintf("CANCEL JOB %d", ji.id)); err != nil {
		t.Fatal(err)
	}
	ji.id = 0
}

func createChangeFeed(
	t *testing.T, db *sql.DB, url string, endpoint string, tis ...tableInfo,
) jobInfo {
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
	if err := crdb.Execute(func() error {
		return db.QueryRow(query.String()).Scan(&jobID)
	}); err != nil {
		t.Fatal(err)
	}
	return jobInfo{
		db: db,
		id: jobID,
	}
}

// dropSinkDB is just a wrapper around DropSinkDB for testing.
func dropSinkDB(t *testing.T, db *sql.DB) {
	if err := DropSinkDB(db); err != nil {
		t.Fatal(err)
	}
}

// createSinkDB will first drop then create a new sink db.
func createSinkDB(t *testing.T, db *sql.DB) {
	dropSinkDB(t, db)
	if err := CreateSinkDB(db); err != nil {
		t.Fatal(err)
	}
}

// TestDB is just a quick test to create and drop a database to ensure the
// Cockroach Cluster is working correctly and we have the correct permissions.
func TestDB(t *testing.T) {
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Find the DB.
	var actualDBName string
	if err := crdb.Execute(func() error {
		return db.QueryRow(
			`SELECT database_name FROM [SHOW DATABASES] WHERE database_name = $1`, dbName,
		).Scan(&actualDBName)
	}); err != nil {
		t.Fatal(err)
	}

	if actualDBName != dbName {
		t.Fatal(fmt.Sprintf("DB names do not match expected - %s, actual: %s", dbName, actualDBName))
	}

	// Create a test table and insert some rows
	table := createTestSimpleTable(t, db, dbName)
	table.populateTable(t, 10)
	if count := table.getTableRowCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}
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
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(t, db)
	defer dropSinkDB(t, db)

	// Create the table to import from
	tableFrom := createTestSimpleTable(t, db, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(t, db, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(t, 10)
	if count := tableFrom.getTableRowCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(t, db, server.URL, endpointTest, tableFrom.tableInfo)
	defer job.cancelJob(t)

	tableFrom.populateTable(t, 10)

	for tableTo.getTableRowCount(t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	tableFrom.populateTable(t, 10)

	for tableTo.getTableRowCount(t) != 30 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	if sinkCount := getRowCount(t, db, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestFeedDelete(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(t, db)
	defer dropSinkDB(t, db)

	// Create the table to import from
	tableFrom := createTestSimpleTable(t, db, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(t, db, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(t, 10)
	if count := tableFrom.getTableRowCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(t, db, server.URL, endpointTest, tableFrom.tableInfo)
	defer job.cancelJob(t)

	tableFrom.populateTable(t, 10)

	for tableTo.getTableRowCount(t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	tableFrom.deleteAll(t)

	for tableTo.getTableRowCount(t) != 0 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	if sinkCount := getRowCount(t, db, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestFeedUpdateNonPrimary(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(t, db)
	defer dropSinkDB(t, db)

	// Create the table to import from
	tableFrom := createTestSimpleTable(t, db, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(t, db, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(t, 10)
	if count := tableFrom.getTableRowCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(t, db, server.URL, endpointTest, tableFrom.tableInfo)
	defer job.cancelJob(t)

	tableFrom.populateTable(t, 10)

	for tableTo.getTableRowCount(t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	tableFrom.updateNoneKeyColumns(t)

	for tableTo.maxB(t) != 2000 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	if sinkCount := getRowCount(t, db, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestFeedUpdatePrimary(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(t, db)
	defer dropSinkDB(t, db)

	// Create the table to import from
	tableFrom := createTestSimpleTable(t, db, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(t, db, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(t, 10)
	if count := tableFrom.getTableRowCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks, err := CreateSinks(db, createConfig(tableFrom.tableInfo, tableTo.tableInfo, endpointTest))
	if err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(db, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(t, db, server.URL, endpointTest, tableFrom.tableInfo)
	defer job.cancelJob(t)

	tableFrom.populateTable(t, 10)

	for tableTo.getTableRowCount(t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	tableFrom.updateAll(t)

	for tableTo.maxB(t) != 2000 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 10)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(endpointTest, tableFrom.name)
	if sinkCount := getRowCount(t, db, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestTypes(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(t, db)
	defer dropSinkDB(t, db)

	// Create the sinks
	sinks, err := CreateSinks(db, []ConfigEntry{})
	if err != nil {
		t.Fatal(err)
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
		{`int_array`, `INT[]`, `{1,2,3}`, false},
		{`serial_array`, `SERIAL[]`, `{148591304110702593,148591304110702594,148591304110702595}`, false},
		{`bit`, `VARBIT`, `10010101`, true},
		{`bool`, `BOOL`, `true`, true},
		// {`bytes`, `BYTES`, `b'\141\061\142\062\143\063'`, true},
		{`collate`, `STRING COLLATE de`, `'a1b2c3' COLLATE de`, true},
		{`date`, `DATE`, `2016-01-25`, true},
		{`decimal`, `DECIMAL`, `1.2345`, true},
		{`float`, `FLOAT`, `1.2345`, true},
		{`inet`, `INET`, `192.168.0.1`, true},
		{`int`, `INT`, `12345`, true},
		{`interval`, `INTERVAL`, `2h30m30s`, true},
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
		{`serial`, `SERIAL`, `148591304110702593`, true},
		{`string`, `STRING`, `a1b2c3`, true},
		{`string_escape`, `STRING`, `a1\b/2?c"3`, true},
		{`time`, `TIME`, `01:23:45.123456`, true},
		{`timestamp`, `TIMESTAMP`, `2016-01-25 10:10:10`, true},
		{`timestamptz`, `TIMESTAMPTZ`, `2016-01-25 10:10:10-05:00`, true},
		{`uuid`, `UUID`, `7f9c24e8-3b12-4fef-91e0-56a2d5a246ec`, true},
	}

	/*
			weird bytes issue
		   {"after":

		   {"a": "\\x62275c3134315c3036315c3134325c3036325c3134335c30363327", "b":
		   "\\x62275c3134315c3036315c3134325c3036325c3134335c30363327"}, "key": ["\\x62275c3134315c3036315c3134325c3036325c3134335c30363327"], "updated": "1586568963316966000.0000000000"}


		   UPSERT INTO _test_db_9945.out_bytes(a, b) VALUES ($1, $2)" $1:"'\\x5c78363232373563333133343331356333303336333135633331333433323563333033363332356333313334333335633330333633333237'",     $2:"'\\x5c78363232373563333133343331356333303336333135633331333433323563333033363332356333313334333335633330333633333237'"
	*/

	tableIndexableSchema := `CREATE TABLE %s (a %s PRIMARY KEY,	b %s)`
	tableNonIndexableSchema := `CREATE TABLE %s (a INT PRIMARY KEY,	b %s)`

	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
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
			tableIn.dropTable(t)
			tableOut.dropTable(t)

			// Create both tables.
			if test.indexable {
				if err := Execute(db, fmt.Sprintf(
					tableIndexableSchema, tableIn.getFullName(), test.columnType, test.columnType,
				)); err != nil {
					t.Fatal(err)
				}
				if err := Execute(db, fmt.Sprintf(
					tableIndexableSchema, tableOut.getFullName(), test.columnType, test.columnType,
				)); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := Execute(db, fmt.Sprintf(
					tableNonIndexableSchema, tableIn.getFullName(), test.columnType,
				)); err != nil {
					t.Fatal(err)
				}
				if err := Execute(db, fmt.Sprintf(
					tableNonIndexableSchema, tableOut.getFullName(), test.columnType,
				)); err != nil {
					t.Fatal(err)
				}
			}

			// Defer a table drop for both tables to clean them up.
			defer tableIn.dropTable(t)
			defer tableOut.dropTable(t)

			// Create the sink
			// There is no way to remove a sink at this time, and that should be ok
			// for these tests.
			if err := sinks.AddSink(db, ConfigEntry{
				Endpoint:            endpointTest,
				DestinationDatabase: dbName,
				DestinationTable:    tableOut.name,
				SourceTable:         tableIn.name,
			}); err != nil {
				t.Fatal(err)
			}

			// Create the CDC feed.
			job := createChangeFeed(t, db, server.URL, endpointTest, tableIn)
			defer job.cancelJob(t)

			// Insert a row into the in table.
			if test.indexable {
				if err := Execute(db,
					fmt.Sprintf("INSERT INTO %s (a,b) VALUES ($1,$2)", tableIn.getFullName()),
					test.columnValue, test.columnValue,
				); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := Execute(db,
					fmt.Sprintf("INSERT INTO %s (a, b) VALUES (1, $1)", tableIn.getFullName()),
					test.columnValue,
				); err != nil {
					t.Fatal(err)
				}
			}

			// Wait until the out table has a row.
			for tableOut.getTableRowCount(t) != 1 {
				// add a stopper here from a wrapper around the handler.
				time.Sleep(time.Millisecond * 10)
			}

			// Now fetch that rows and compare them.
			var inA, inB interface{}
			if err := crdb.Execute(func() error {
				return db.QueryRow(
					fmt.Sprintf("SELECT a, b FROM %s LIMIT 1", tableIn.getFullName()),
				).Scan(&inA, &inB)
			}); err != nil {
				t.Fatal(err)
			}
			var outA, outB interface{}
			if err := crdb.Execute(func() error {
				return db.QueryRow(
					fmt.Sprintf("SELECT a, b FROM %s LIMIT 1", tableOut.getFullName()),
				).Scan(&outA, &outB)
			}); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprintf("%v", inA) != fmt.Sprintf("%v", outA) {
				t.Errorf("A: expected %v, got %v", inA, outA)
			}
			if fmt.Sprintf("%v", inB) != fmt.Sprintf("%v", outB) {
				t.Errorf("B: expected %v, got %v", inB, outB)
			}
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
			actual, err := parseConfig(test.testJSON)
			if test.expectedPass != (err == nil) {
				t.Errorf("Expected %t, actual %t", test.expectedPass, err == nil)
				if err != nil {
					t.Errorf("Got error: %s", err.Error())
				}
			}
			if test.expectedPass && !reflect.DeepEqual(test.expectedConfig, actual) {
				t.Errorf("Expected %+v, actual %+v", test.expectedConfig, actual)
			}
		})
	}
}

func TestMultipleFeeds(t *testing.T) {
	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	testcases := []struct {
		feedCount     int
		tablesPerFeed int
	}{
		{1, 1},
		{1, 2},
		{1, 3},
		{2, 1},
		{2, 2},
		{2, 3},
		{3, 1},
		{3, 2},
		{3, 3},
	}

	nameEndpoint := func(feedID int) string {
		return fmt.Sprintf("test_%d_%s", feedID, endpointTest)
	}

	for _, testcase := range testcases {
		t.Run(fmt.Sprintf("Feeds_%d_Tables_%d", testcase.feedCount, testcase.tablesPerFeed), func(t *testing.T) {

			// Create a new _cdc_sink db
			createSinkDB(t, db)
			defer dropSinkDB(t, db)

			// Create all the tables
			var sourceTablesByFeed [][]*tableInfoSimple
			var destinationTablesByFeed [][]*tableInfoSimple
			for i := 0; i < testcase.feedCount; i++ {
				var sourceTables []*tableInfoSimple
				var destinationTables []*tableInfoSimple
				for j := 0; j < testcase.tablesPerFeed; j++ {
					sourceTable := createTestSimpleTable(t, db, dbName)
					sourceTables = append(sourceTables, &sourceTable)
					destinationTable := createTestSimpleTable(t, db, dbName)
					destinationTables = append(destinationTables, &destinationTable)
				}
				sourceTablesByFeed = append(sourceTablesByFeed, sourceTables)
				destinationTablesByFeed = append(destinationTablesByFeed, destinationTables)
			}

			// Populate all the source tables
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					table.populateTable(t, 10)
				}
			}

			// Create the sinks
			sinks, err := CreateSinks(db, []ConfigEntry{})
			if err != nil {
				t.Fatal(err)
			}

			// Create all the sinks
			for i := 0; i < testcase.feedCount; i++ {
				for j := 0; j < testcase.tablesPerFeed; j++ {
					if err := sinks.AddSink(db, ConfigEntry{
						Endpoint:            nameEndpoint(i),
						DestinationDatabase: destinationTablesByFeed[i][j].dbName,
						DestinationTable:    destinationTablesByFeed[i][j].name,
						SourceTable:         sourceTablesByFeed[i][j].name,
					}); err != nil {
						t.Fatal(err)
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
				job := createChangeFeed(t, db, server.URL, nameEndpoint(i), tableInfos...)
				defer job.cancelJob(t)
			}

			// Add some more lines to each table.
			// Populate all the source tables
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					table.populateTable(t, 10)
				}
			}

			// Make sure each table has 20 rows
			for _, feedTables := range destinationTablesByFeed {
				for _, table := range feedTables {
					for table.getTableRowCount(t) != 20 {
						time.Sleep(time.Millisecond * 10)
					}
				}
			}

			// Update all rows in the source table.
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					table.updateAll(t)
				}
			}

			// Make sure each table has 20 rows
			for _, feedTables := range destinationTablesByFeed {
				for _, table := range feedTables {
					for table.maxB(t) != 2000 {
						time.Sleep(time.Millisecond * 10)
					}
				}
			}

			// Delete all rows in the table.
			// Update all rows in the source table.
			for _, feedTables := range sourceTablesByFeed {
				for _, table := range feedTables {
					table.deleteAll(t)
				}
			}

			// Make sure each table has 20 rows
			for _, feedTables := range destinationTablesByFeed {
				for _, table := range feedTables {
					for table.getTableRowCount(t) != 0 {
						time.Sleep(time.Millisecond * 10)
					}
				}
			}
		})
	}
}
