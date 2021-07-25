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
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
)

// Sinks holds a map of all known sinks.
type Sinks struct {
	// Is this mutex overkill?  Is it needed? There should never be any writes
	// to the map after initialization. But guidance here is fuzzy, so I'll keep
	// it in.
	sync.RWMutex

	// endpoints can have multiple tables
	sinksByTableByEndpoint map[string]map[string]*Sink
}

// CreateSinks creates a new table sink and populates it based on the pass in
// config.
func CreateSinks(ctx context.Context, db *pgxpool.Pool, config Config) (*Sinks, error) {
	sinks := &Sinks{
		sinksByTableByEndpoint: make(map[string]map[string]*Sink),
	}

	for _, entry := range config {
		if err := sinks.AddSink(ctx, db, entry); err != nil {
			return nil, err
		}
	}

	return sinks, nil
}

// AddSink creates and adds a new sink to the sinks map.
func (s *Sinks) AddSink(ctx context.Context, db *pgxpool.Pool, entry ConfigEntry) error {
	s.Lock()
	defer s.Unlock()

	sourceTable := strings.ToLower(strings.TrimSpace(entry.SourceTable))
	destinationDB := strings.ToLower(strings.TrimSpace(entry.DestinationDatabase))
	destinationTable := strings.ToLower(strings.TrimSpace(entry.DestinationTable))
	endpoint := strings.ToLower(strings.TrimSpace(entry.Endpoint))

	// First check to make sure the endpoint exists, if it doesn't create one.
	var sinksByTable map[string]*Sink
	var exist bool
	if sinksByTable, exist = s.sinksByTableByEndpoint[endpoint]; !exist {
		sinksByTable = make(map[string]*Sink)
		s.sinksByTableByEndpoint[endpoint] = sinksByTable
	}

	// Check for a double table
	if _, exist := sinksByTable[sourceTable]; exist {
		return fmt.Errorf("Duplicate table configuration entry found: %s", sourceTable)
	}

	sink, err := CreateSink(ctx, db, sourceTable, destinationDB, destinationTable, endpoint)
	if err != nil {
		return err
	}
	sinksByTable[sourceTable] = sink
	s.sinksByTableByEndpoint[endpoint] = sinksByTable
	return nil
}

// FindSink returns a sink for a given table name and endpoint.
func (s *Sinks) FindSink(endpoint string, table string) *Sink {
	s.RLock()
	defer s.RUnlock()
	sinksByTable, exist := s.sinksByTableByEndpoint[endpoint]
	if !exist {
		return nil
	}
	result, _ := sinksByTable[table]
	return result
}

// GetAllSinksByEndpoint gets a list of all known sinks.
func (s *Sinks) GetAllSinksByEndpoint(endpoint string) []*Sink {
	s.RLock()
	defer s.RUnlock()
	var allSinks []*Sink
	if sinksByTable, exists := s.sinksByTableByEndpoint[endpoint]; exists {
		for _, sink := range sinksByTable {
			allSinks = append(allSinks, sink)
		}
	}
	return allSinks
}

// HandleResolvedRequest parses and applies all the resolved upserts.
func (s *Sinks) HandleResolvedRequest(
	ctx context.Context, db *pgxpool.Pool, rURL resolvedURL, w http.ResponseWriter, r *http.Request,
) {
	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()
	for scanner.Scan() {
		next, err := parseResolvedLine(scanner.Bytes(), rURL.endpoint)
		if err != nil {
			log.Print(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Start the transation
		if err := Retry(ctx, func(ctx context.Context) error {
			tx, err := db.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			// Get the previous resolved
			prev, err := getPreviousResolved(ctx, tx, rURL.endpoint)
			if err != nil {
				return err
			}
			log.Printf("%s: resolved - timestamp %d.%d", next.endpoint, next.nanos, next.logical)

			// Find all rows to update and upsert them.
			allSinks := s.GetAllSinksByEndpoint(rURL.endpoint)
			for _, sink := range allSinks {
				if err := sink.UpdateRows(ctx, tx, prev, next); err != nil {
					return err
				}
			}

			// Write the updated resolved.
			if err := next.writeUpdated(ctx, tx); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}); err != nil {
			log.Print(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
