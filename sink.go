package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// Sink holds all the info needed for a specific table.
type Sink struct {
	originalTable string
	resultDBTable string
	sinkDBTable   string
}

// CreateSink creates all the required tables and returns a new Sink.
func CreateSink(
	db *sql.DB, originalTable string, resultDB string, resultTable string,
) (*Sink, error) {
	// Check to make sure the table exists.
	resultDBTable := fmt.Sprintf("%s.%s", resultDB, resultTable)
	exists, err := TableExists(db, resultDB, resultTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("Table %s could not be found", resultDBTable)
	}

	// Grab all the columns here.
	sinkDBTable := SinkDBTableName(resultDB, resultTable)
	if err := CreateSinkTable(db, sinkDBTable); err != nil {
		return nil, err
	}

	sink := &Sink{
		originalTable: originalTable,
		resultDBTable: resultDBTable,
		sinkDBTable:   sinkDBTable,
	}

	return sink, nil
}
