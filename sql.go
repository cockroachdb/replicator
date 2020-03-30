package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func createSinkDB(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *sinkDB))
	return err
}

const sinkTableSchema = `
CREATE TABLE IF NOT EXISTS %s.%s (
	updated TIMESTAMP PRIMARY KEY,
	key STRING,
	after STRING
)
`

func createTempTables(db *sql.DB) error {
	sinkTable := fmt.Sprintf(sinkTableSchema, *sinkDB, *resultTable)
	log.Printf("Sink Table: %s", sinkTable)
	_, err := db.Exec(sinkTable)
	return err
}

func dropSinkDB(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(`DROP DATABASE %s CASCADE`, *sinkDB))
	return err
}
