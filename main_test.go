package main

import (
	"database/sql"
	"fmt"
	"math/rand"
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

}
