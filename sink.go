package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

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

// UpsertRows upserts all changed rows.
func (s *Sink) UpsertRows(tx *sql.Tx, prev ResolvedLine, next ResolvedLine) error {
	log.Printf("Updating Sink %s", s.resultTableFullName)

	// First, gather all the rows to update.
	lines, err := FindAllRowsToUpdate(tx, s.sinkTableFullName, prev, next)
	if err != nil {
		return err
	}
	for _, line := range lines {
		log.Printf("line to update: %+v", line)
	}

	for _, line := range lines {
		// Parse the key into columns
		if err := json.Unmarshal([]byte(line.key), &(line.Key)); err != nil {
			return err
		}
		// Is this needed?  What if we have 2 primary key columns but the 2nd one
		// nullable or has a default?  Does CDC send it?
		if len(line.Key) != len(s.primaryKeyColumns) {
			return fmt.Errorf(
				"table %s has %d primary key columns %v, but only got %d keys %v",
				s.resultTableFullName,
				len(s.primaryKeyColumns),
				s.primaryKeyColumns,
				len(line.Key),
				line.Key,
			)
		}

		// Parse the after columns
		if err := json.Unmarshal([]byte(line.after), &(line.After)); err != nil {
			return err
		}

		// Find all the columns that need to be part of the upsert.
		columns := make(map[string]interface{})
		for name, value := range line.After {
			columns[name] = value
		}
		for i, column := range s.primaryKeyColumns {
			columns[column] = line.Key[i]
		}

		// Build the statement.
		var statement, values strings.Builder
		fmt.Fprintf(&statement, "UPSERT INTO %s (", s.resultTableFullName)
		first := true
		for name, value := range columns {
			if first {
				first = false
			} else {
				fmt.Fprint(&statement, ", ")
				fmt.Fprint(&values, ", ")
			}
			fmt.Fprint(&statement, name)
			fmt.Fprint(&values, value)
		}
		fmt.Fprintf(&statement, ") VALUES (%s)", values.String())
		log.Printf("Upsert Statement: %s", statement.String())

		// Upsert the line
		if _, err := tx.Exec(statement.String()); err != nil {
			return err
		}

		// Delete the line from the sink table.
		line.DeleteLine(tx, s.sinkTableFullName)
	}

	return nil
}
