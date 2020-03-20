package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func TestDB(t *testing.T) {
	db, err := sql.Open("postgres", *connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the "accounts" table.
	dbNum := r.Intn(10000)
	dbName := fmt.Sprintf("_test_db_%d", dbNum)

	t.Log(dbName)

	if _, err := db.Exec(
		`CREATE DATABASE IF NOT EXISTS ` + dbName); err != nil {
		t.Fatal(err)
	}

	// Find the DB.
	var actualDBName string
	row := db.QueryRow(`SELECT database_name FROM [SHOW DATABASES] WHERE database_name = $1`, dbName)
	if err := row.Scan(&actualDBName); err != nil {
		t.Fatal(err)
	}

	if actualDBName != dbName {
		t.Fatal(fmt.Sprintf("DB names do not match expected - %s, actual: %s", dbName, actualDBName))
	}

	if _, err := db.Exec(
		`DROP DATABASE ` + dbName + ` CASCADE`); err != nil {
		t.Fatal(err)
	}
}
