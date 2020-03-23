package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

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

	if _, err := db.Exec(
		`CREATE DATABASE IF NOT EXISTS ` + dbName); err != nil {
		t.Fatal(err)
	}

	closer = func() {
		if _, err := db.Exec(
			`DROP DATABASE ` + dbName + ` CASCADE`); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	return
}

const tableStyle1 = `
CREATE TABLE %s.%s (
	a INT PRIMARY KEY,
	b INT
)
`

type tableInfo struct {
	db       *sql.DB
	dbName   string
	name     string
	rowCount int
}

type jobInfo struct {
	db *sql.DB
	id int
}

func (ji *jobInfo) cancelJob(t *testing.T) {
	if ji.id == 0 {
		return
	}
	if _, err := ji.db.Exec(fmt.Sprintf("CANCEL JOB %d", ji.id)); err != nil {
		t.Fatal(err)
	}
	ji.id = 0
}

func (ti tableInfo) getFullName() string {
	return fmt.Sprintf("%s.%s", ti.dbName, ti.name)
}

func (ti *tableInfo) populateTable(t *testing.T, count int) {
	for i := 0; i < count; i++ {
		if _, err := ti.db.Exec("INSERT INTO "+ti.getFullName()+" VALUES ($1,$1)", ti.rowCount+1); err != nil {
			t.Fatal(err)
		}
		ti.rowCount++
	}
}

func (ti tableInfo) getTableCount(t *testing.T) int {
	row := ti.db.QueryRow("SELECT COUNT(*) FROM " + ti.getFullName())
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (ti tableInfo) createChangeFeed(t *testing.T, url string) jobInfo {
	query := fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s INTO 'experimental-%s/test.sql'", ti.getFullName(), url)
	t.Logf(query)
	row := ti.db.QueryRow(
		fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s INTO 'experimental-%s/test.sql'", ti.getFullName(), url),
	)
	var jobID int
	if err := row.Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	return jobInfo{
		db: ti.db,
		id: jobID,
	}
}

// This function creates a test table and returns its name.
func createTestTable(t *testing.T, db *sql.DB, dbName string) tableInfo {
	var tableName string

outer:
	for {
		// Create the testing database
		tableNum := r.Intn(10000)
		tableName = fmt.Sprintf("_test_table_%d", tableNum)

		// Find the DB.
		var actualTableName string
		row := db.QueryRow(
			fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = $1", dbName),
			tableName,
		)
		err := row.Scan(&actualTableName)
		switch err {
		case sql.ErrNoRows:
			break outer
		case nil:
			continue
		default:
			t.Fatal(err)
		}
	}

	if _, err := db.Exec(
		fmt.Sprintf(tableStyle1, dbName, tableName)); err != nil {
		t.Fatal(err)
	}

	t.Logf("Testing Table: %s.%s", dbName, tableName)
	return tableInfo{
		db:     db,
		dbName: dbName,
		name:   tableName,
	}
}

// TestDB is just a quick test to create and drop a database to ensure the
// Cockroach Cluster is working correctly and we have the correct permissions.
func TestDB(t *testing.T) {
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Find the DB.
	var actualDBName string
	row := db.QueryRow(`SELECT database_name FROM [SHOW DATABASES] WHERE database_name = $1`, dbName)
	if err := row.Scan(&actualDBName); err != nil {
		t.Fatal(err)
	}

	if actualDBName != dbName {
		t.Fatal(fmt.Sprintf("DB names do not match expected - %s, actual: %s", dbName, actualDBName))
	}

	// Create a test table and insert some rows
	table := createTestTable(t, db, dbName)
	table.populateTable(t, 10)
	if count := table.getTableCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}
}

func TestFeedImport(t *testing.T) {

	// Create the test db
	db, dbName, dbClose := getDB(t)
	defer dbClose()

	// Create a test http server
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	// Create the test table, give it a few rows.
	table := createTestTable(t, db, dbName)
	table.populateTable(t, 10)
	if count := table.getTableCount(t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	job := table.createChangeFeed(t, server.URL)
	defer job.cancelJob(t)

	/*client := server.Client()
	  content := strings.NewReader("my request")
	  resp, err := client.Post(server.URL, "text/html", content)
	  if err != nil {
	  	t.Fatal(err)
	  }
	  if resp.StatusCode != http.StatusOK {
	  	t.Fatalf("Got Response Code: %d", resp.StatusCode)
	  }*/

}
