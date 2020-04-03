package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

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

// Line is used to parse a json line in the request body.
//{"after": {"a": 1, "b": 1}, "key": [1], "updated": "1585949214695218000.0000000000"}
type Line struct {
	After     map[string]interface{} `json:"after"`
	Key       []interface{}          `json:"key"`
	Updated   string                 `json:"updated"`
	timestamp time.Time
	logical   int
}

// HandleRequest is a handler used for this specific sink.
func (s *Sink) HandleRequest(w http.ResponseWriter, r *http.Request) {
	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()
	for scanner.Scan() {
		var line Line
		json.Unmarshal(scanner.Bytes(), &line)
		//log.Printf("body: %s\n", scanner.Text())
		log.Printf("line: %+v\n", line)
		var err error
		line.timestamp, line.logical, err = parseSplitTimestamp(line.Updated)
		if err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}
