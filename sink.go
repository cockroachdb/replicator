// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Sink holds all the info needed for a specific table.
type Sink struct {
	originalTableName   string
	resultTableFullName string
	sinkTableFullName   string
	primaryKeyColumns   []string
	endpoint            string
	ignoredColumns      map[string]struct{}
}

// CreateSink creates all the required tables and returns a new Sink.
func CreateSink(
	ctx context.Context, db *pgxpool.Pool,
	originalTable string, resultDB string, resultTable string, endpoint string,
) (*Sink, error) {
	// Check to make sure the table exists.
	resultTableFullName := fmt.Sprintf("%s.%s", resultDB, resultTable)
	exists, err := TableExists(ctx, db, resultDB, resultTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("table %s could not be found", resultTableFullName)
	}

	sinkTableFullName := SinkTableFullName(resultDB, resultTable)
	if err := CreateSinkTable(ctx, db, sinkTableFullName); err != nil {
		return nil, err
	}

	columns, err := GetPrimaryKeyColumns(ctx, db, resultTableFullName)
	if err != nil {
		return nil, err
	}

	toIgnore, err := GetIgnoredColumns(ctx, db, resultTableFullName)
	if err != nil {
		return nil, err
	}
	ignoreMap := make(map[string]struct{}, len(toIgnore))
	for _, col := range toIgnore {
		ignoreMap[col] = struct{}{}
	}

	sink := &Sink{
		originalTableName:   originalTable,
		resultTableFullName: resultTableFullName,
		sinkTableFullName:   sinkTableFullName,
		primaryKeyColumns:   columns,
		endpoint:            endpoint,
		ignoredColumns:      ignoreMap,
	}

	return sink, nil
}

const chunkSize = 1000

// HandleRequest is a handler used for this specific sink.
func (s *Sink) HandleRequest(db *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()
	var lines []Line
	for scanner.Scan() {
		line, err := parseLine(scanner.Bytes())
		if err != nil {
			log.Print(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		lines = append(lines, line)
		if len(lines) >= chunkSize {
			if err := WriteToSinkTable(r.Context(), db, s.sinkTableFullName, lines); err != nil {
				log.Print(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			log.Printf("%s: added %d operations", s.endpoint, chunkSize)
			lines = []Line{}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := WriteToSinkTable(r.Context(), db, s.sinkTableFullName, lines); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("%s: added %d operations", s.endpoint, len(lines))
}

// deleteRows preforms all the deletes specified in lines.
func (s *Sink) deleteRows(ctx context.Context, tx pgxtype.Querier, lines []Mutation) error {
	if len(lines) == 0 {
		return nil
	}

	var chunks [][]Mutation
	for i := 0; i < len(lines); i += chunkSize {
		end := i + chunkSize
		if end > len(lines) {
			end = len(lines)
		}
		chunks = append(chunks, lines[i:end])
	}

	for _, chunk := range chunks {
		// Build the statement.
		var statement strings.Builder
		fmt.Fprintf(&statement, "DELETE FROM %s WHERE (", s.resultTableFullName)
		for i, column := range s.primaryKeyColumns {
			if i > 0 {
				fmt.Fprint(&statement, ",")
			}
			// Placeholder index always starts at 1.
			fmt.Fprintf(&statement, "%s", column)
		}
		fmt.Fprintf(&statement, ") IN (")
		var keys []interface{}
		for i, line := range chunk {
			// Parse out the primary key values.
			key := make([]interface{}, 0, len(s.primaryKeyColumns))
			dec := json.NewDecoder(bytes.NewReader(line.key))
			dec.UseNumber()
			if err := dec.Decode(&key); err != nil {
				return err
			}

			if i > 0 {
				fmt.Fprintf(&statement, ",")
			}
			fmt.Fprintf(&statement, "(")
			for i, key := range key {
				if i > 0 {
					fmt.Fprintf(&statement, ",")
				}
				keys = append(keys, key)
				fmt.Fprintf(&statement, "$%d", len(keys))
			}
			fmt.Fprintf(&statement, ")")
		}
		fmt.Fprintf(&statement, ")")

		// Upsert the line
		if _, err := tx.Exec(ctx, statement.String(), keys...); err != nil {
			return err
		}
	}
	return nil
}

// upsertRows performs all upserts specified in lines.
func (s *Sink) upsertRows(ctx context.Context, tx pgxtype.Querier, lines []Mutation) error {
	const starterColumns = 16
	if len(lines) == 0 {
		return nil
	}

	// Get all the column names and order them alphabetically.
	allNames, err := lines[0].extractColumns(make([]string, 0, starterColumns))
	if err != nil {
		return err
	}

	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	columnNames := allNames[:0]
	for _, name := range allNames {
		if _, ignored := s.ignoredColumns[name]; !ignored {
			columnNames = append(columnNames, name)
		}
	}
	sort.Strings(columnNames)

	var chunks [][]Mutation
	for i := 0; i < len(lines); i += chunkSize {
		end := i + chunkSize
		if end > len(lines) {
			end = len(lines)
		}
		chunks = append(chunks, lines[i:end])
	}

	for _, chunk := range chunks {
		// Build the statement.
		var statement strings.Builder
		// TODO: This first part can be memoized as long as there are no schema
		// changes.
		fmt.Fprintf(&statement, "UPSERT INTO %s (", s.resultTableFullName)

		for i, name := range columnNames {
			if i > 0 {
				fmt.Fprintf(&statement, ",")
			}
			fmt.Fprint(&statement, name)
		}
		fmt.Fprint(&statement, ") VALUES ")

		var values []interface{}
		for i, line := range chunk {
			data := make(map[string]interface{}, starterColumns)
			if err := line.parseAfter(data); err != nil {
				return nil
			}
			if i == 0 {
				fmt.Fprintf(&statement, "(")
			} else {
				fmt.Fprintf(&statement, ",(")
			}
			for j, name := range columnNames {
				values = append(values, data[name])
				if j == 0 {
					fmt.Fprintf(&statement, "$%d", len(values))
				} else {
					fmt.Fprintf(&statement, ",$%d", len(values))
				}
			}
			fmt.Fprintf(&statement, ")")
		}

		// Upsert the line
		if _, err := tx.Exec(ctx, statement.String(), values...); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRows updates all changed rows.
func (s *Sink) UpdateRows(ctx context.Context, tx pgxtype.Querier, prev ResolvedLine, next ResolvedLine) error {
	// First, gather all the rows to update.
	mutations, err := DrainMutations(ctx, tx, s.sinkTableFullName, prev, next)
	if err != nil {
		return err
	}

	if len(mutations) == 0 {
		return nil
	}

	log.Printf("%s: %s executed %d operations", s.endpoint, s.sinkTableFullName, len(mutations))

	// TODO: Batch these by 100 rows?  Not sure what the max should be.

	var upserts []Mutation
	var deletes []Mutation

	for _, mut := range mutations {
		// Parse the key into columns
		// Large numbers are not turned into strings, so the UseNumber option for
		// the decoder is required.
		key := make([]interface{}, 0, len(s.primaryKeyColumns))
		dec := json.NewDecoder(bytes.NewReader(mut.key))
		dec.UseNumber()
		if err := dec.Decode(&key); err != nil {
			return err
		}

		// Is this needed?  What if we have 2 primary key columns but the 2nd one
		// nullable or has a default?  Does CDC send it?
		if len(key) != len(s.primaryKeyColumns) {
			return fmt.Errorf(
				"table %s has %d primary key columns %v, but only got %d keys %v",
				s.resultTableFullName,
				len(s.primaryKeyColumns),
				s.primaryKeyColumns,
				len(key),
				key,
			)
		}

		// Is this a delete?
		if string(mut.after) == "null" {
			deletes = append(deletes, mut)
		} else {
			// This must be an upsert statement.
			upserts = append(upserts, mut)
		}

	}

	// Delete all rows
	if err := s.deleteRows(ctx, tx, deletes); err != nil {
		return err
	}

	// Upsert all rows
	return s.upsertRows(ctx, tx, upserts)
}
