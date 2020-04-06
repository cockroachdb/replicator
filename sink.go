package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

// Sink holds all the info needed for a specific table.
type Sink struct {
	originalTableName   string
	resultTableFullName string
	sinkTableFullName   string
	primaryKeyColumns   []string
}

// CreateSink creates all the required tables and returns a new Sink.
func CreateSink(
	db *sql.DB, originalTable string, resultDB string, resultTable string,
) (*Sink, error) {
	// Check to make sure the table exists.
	resultTableFullName := fmt.Sprintf("%s.%s", resultDB, resultTable)
	exists, err := TableExists(db, resultDB, resultTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("Table %s could not be found", resultTableFullName)
	}

	sinkTableFullName := SinkTableFullName(resultDB, resultTable)
	if err := CreateSinkTable(db, sinkTableFullName); err != nil {
		return nil, err
	}

	columns, err := GetPrimaryKeyColumns(db, resultTableFullName)
	if err != nil {
		return nil, err
	}

	sink := &Sink{
		originalTableName:   originalTable,
		resultTableFullName: resultTableFullName,
		sinkTableFullName:   sinkTableFullName,
		primaryKeyColumns:   columns,
	}

	return sink, nil
}

// HandleRequest is a handler used for this specific sink.
func (s *Sink) HandleRequest(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()
	for scanner.Scan() {
		line, err := parseLine(scanner.Bytes())
		if err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := line.WriteToSinkTable(db, s.sinkTableFullName); err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
}

// FindAllRowsToUpdate returns all the rows that need to be updated from the
// sink table.
func (s *Sink) FindAllRowsToUpdate(
	tx *sql.Tx, prev ResolvedLine, next ResolvedLine,
) ([]Line, error) {
	return FindAllRowsToUpdate(tx, s.sinkTableFullName, prev, next)
}
