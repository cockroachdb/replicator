package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

// CreateSinkDB creates a new sink db if one does not exist yet and also adds
// the resolved table.
func CreateSinkDB(db *sql.DB) error {
	// Needs retry.
	// TODO - Set the zone configs to be small here.
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *sinkDB))
	if err != nil {
		return err
	}

	return CreateResolvedTable(db)
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
	log.Printf(findTableSQL)
	row := db.QueryRow(findTableSQL)
	var tableFound string
	err := row.Scan(&tableFound)
	switch err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		log.Printf("Found: %s", tableFound)
		return true, nil
	default:
		return false, err
	}
}
