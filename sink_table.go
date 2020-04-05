package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

const sinkTableSchema = `
CREATE TABLE IF NOT EXISTS %s (
	nanos INT NOT NULL,
  logical INT NOT NULL,
	key STRING NOT NULL,
	after STRING,
	PRIMARY KEY (nanos, logical)
)
`
const writeSinkTable = `UPSERT INTO %s (nanos, logical, key, after) VALUES ($1, $2, $3, $4)`

// SinkDBTableName creates the conjoined db/table name to be used by the sink
// table.
func SinkDBTableName(resultDB string, resultTable string) string {
	return fmt.Sprintf("%s.%s_%s", *sinkDB, resultDB, resultTable)
}

// Line is used to parse a json line in the request body.
//{"after": {"a": 1, "b": 1}, "key": [1], "updated": "1585949214695218000.0000000000"}
type Line struct {
	// These are used for parsing the ndjson line.
	After   map[string]interface{} `json:"after"`
	Key     []interface{}          `json:"key"`
	Updated string                 `json:"updated"`

	// These are used for storing back to the sink table.
	nanos   int64
	logical int
	after   string
	key     string
}

// parseSplitTimestamp splits a timestmap of tte format NNNN.LLL into an int64
// for the nanos and an int for the logical component.
func parseSplitTimestamp(timestamp string) (int64, int, error) {
	splits := strings.Split(timestamp, ".")
	if len(splits) != 2 {
		return 0, 0, fmt.Errorf("can't parse timestamp %s", timestamp)
	}
	nanos, err := strconv.ParseInt(splits[0], 0, 0)
	if err != nil {
		return 0, 0, err
	}
	if nanos <= 0 {
		return 0, 0, fmt.Errorf("nanos must be greater than 0: %d", nanos)
	}
	logical, err := strconv.Atoi(splits[1])
	if err != nil {
		return 0, 0, err
	}
	return nanos, logical, nil
}

// parseLine takes a single line from an ndjson and parses it into json then converts some of
// the components back to json for storage in the sink table. This parsing back and forth just
// seemed safer than manually parsing the line json.
func parseLine(rawBytes []byte) (Line, error) {
	var line Line
	json.Unmarshal(rawBytes, &line)

	// Prase the timestamp into nanos and logical.
	var err error
	line.nanos, line.logical, err = parseSplitTimestamp(line.Updated)
	if err != nil {
		return Line{}, err
	}
	if line.nanos == 0 {
		return Line{}, fmt.Errorf("no nano component to the 'updated' timestamp field")
	}

	// Convert the after line back to json.
	afterBytes, err := json.Marshal(line.After)
	if err != nil {
		return Line{}, err
	}
	if len(afterBytes) == 0 {
		return Line{}, fmt.Errorf("no value present in 'after' field")
	}
	line.after = string(afterBytes)

	// Convert the key line back to json.
	if len(line.Key) <= 0 {
		return Line{}, fmt.Errorf("no value present in 'key' field")
	}
	keyBytes, err := json.Marshal(line.Key)
	if err != nil {
		return Line{}, err
	}
	if len(keyBytes) == 0 {
		return Line{}, fmt.Errorf("no value present in 'key' field")
	}
	line.key = string(keyBytes)

	log.Printf("line: %+v\n", line)
	return line, err
}

// CreateSinkTable creates if it does not exist, the a table used for sinking.
func CreateSinkTable(db *sql.DB, sinkDBTable string) error {
	// Needs retry.
	_, err := db.Exec(fmt.Sprintf(sinkTableSchema, sinkDBTable))
	return err
}

// WriteToSinkTable upserts a single line to the sink table.
func (line Line) WriteToSinkTable(db *sql.DB, sinkDBTable string) error {
	// Needs retry.
	_, err := db.Exec(fmt.Sprintf(writeSinkTable, sinkDBTable),
		line.nanos, line.logical, line.key, line.after,
	)
	return err
}
