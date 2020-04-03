package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/prometheus/common/log"
)

// CreateSinkDB creates a new sink db if one does not exist yet.
func CreateSinkDB(db *sql.DB) error {
	// Needs retry.
	// TODO - Set the zone configs to be small here.
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *sinkDB))
	return err
}

// DropSinkDB drops the sinkDB and all data in it.
func DropSinkDB(db *sql.DB) error {
	// Needs retry.
	_, err := db.Exec(fmt.Sprintf(`DROP DATABASE %s CASCADE`, *sinkDB))
	return err
}

// TableExists checks for the existence of a table.
func TableExists(db *sql.DB, dbName string, tableName string) (bool, error) {
	// Needs retry.
	findTableSQL := fmt.Sprintf(
		"SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = '%s'",
		dbName, tableName,
	)
	log.Infof(findTableSQL)
	row := db.QueryRow(findTableSQL)
	var tableFound string
	err := row.Scan(&tableFound)
	switch err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		log.Infof("Found: %s", tableFound)
		return true, nil
	default:
		return false, err
	}
}

// SinkDBTableName creates the conjoined db/table name to be used by the sink table.
func SinkDBTableName(resultDB string, resultTable string) string {
	return fmt.Sprintf("%s.%s_%s", *sinkDB, resultDB, resultTable)
}

const sinkTableSchema = `
CREATE TABLE IF NOT EXISTS %s (
	updated TIMESTAMP PRIMARY KEY,
	key STRING,
	after STRING
)
`

// CreateSinkTable creates if it does not exist, the a table used for sinking.
func CreateSinkTable(db *sql.DB, sinkDBTable string) error {
	// Needs retry.
	_, err := db.Exec(fmt.Sprintf(sinkTableSchema, sinkDBTable))
	return err
}
