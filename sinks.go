package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
)

// Sinks holds a map of all known sinks.
type Sinks struct {
	// Is this mutex overkill?  Is it needed? There should never be any writes
	// to the map after initialization. But guidance here is fuzzy, so I'll keep
	// it in.
	sync.RWMutex

	// TODO: Consider changing this to just sinksByTableByEndpoint
	// map[string]map[string]*Sink when allowing multiple endpoints.
	sinksBySourceTable map[string]*Sink
	tablesByEndpoint   map[string][]string
}

// CreateSinks creates a new table sink and populates it based on the pass in
// config.
func CreateSinks(db *sql.DB, config Config) (*Sinks, error) {
	sinks := &Sinks{
		sinksBySourceTable: make(map[string]*Sink),
		tablesByEndpoint:   make(map[string][]string),
	}

	for _, entry := range config {
		if err := sinks.AddSink(db, entry); err != nil {
			return nil, err
		}
	}

	return sinks, nil
}

// AddSink creates and adds a new sink to the sinks map.
func (s *Sinks) AddSink(db *sql.DB, entry ConfigEntry) error {
	s.Lock()
	defer s.Unlock()

	sourceTable := strings.ToLower(strings.TrimSpace(entry.SourceTable))
	destinationDB := strings.ToLower(strings.TrimSpace(entry.DestinationDatabase))
	destinationTable := strings.ToLower(strings.TrimSpace(entry.DestinationTable))
	endpoint := strings.TrimSpace(entry.Endpoint)

	// Check for a double table
	if _, exist := s.sinksBySourceTable[sourceTable]; exist {
		return fmt.Errorf("Duplicate table configuration entry found: %s", sourceTable)
	}

	sink, err := CreateSink(db, sourceTable, destinationDB, destinationTable)
	if err != nil {
		return err
	}
	s.sinksBySourceTable[sourceTable] = sink

	endpointTables, exist := s.tablesByEndpoint[endpoint]
	if !exist {
		endpointTables = []string{}
	}
	endpointTables = append(endpointTables, sourceTable)
	s.tablesByEndpoint[endpoint] = endpointTables

	return nil
}

// FindSinkByTable returns a sink for a given table name.
func (s *Sinks) FindSinkByTable(table string) *Sink {
	s.RLock()
	defer s.RUnlock()
	result, _ := s.sinksBySourceTable[table]
	return result
}

// GetAllSinks gets a list of all known sinks.
func (s *Sinks) GetAllSinks() []*Sink {
	s.RLock()
	defer s.RUnlock()
	var allSinks []*Sink
	for _, sink := range s.sinksBySourceTable {
		allSinks = append(allSinks, sink)
	}
	return allSinks
}

// HandleResolvedRequest parses and applies all the resolved upserts.
func (s *Sinks) HandleResolvedRequest(
	db *sql.DB, rURL resolvedURL, w http.ResponseWriter, r *http.Request,
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
		if err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
			// Get the previous resolved
			prev, err := getPreviousResolved(tx, rURL.endpoint)
			if err != nil {
				return err
			}
			log.Printf("Previous Resolved: %+v, Current Resolved: %+v", prev, next)

			// Find all rows to update and upsert them.
			allSinks := s.GetAllSinks()
			for _, sink := range allSinks {
				if err := sink.UpdateRows(tx, prev, next); err != nil {
					return err
				}
			}

			// Write the updated resolved.
			return next.writeUpdated(tx)
		}); err != nil {
			log.Print(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
