package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	_ "github.com/lib/pq"
)

// Sinks holds a map of all known sinks.
type Sinks struct {
	// Is this mutex overkill?  Is it needed?
	sync.RWMutex
	sinks map[string]*Sink
}

// CreateSinks creates a new table sink.
func CreateSinks() *Sinks {
	return &Sinks{
		sinks: make(map[string]*Sink),
	}
}

// AddSink creates and adds a new sink to the sinks map.
func (s *Sinks) AddSink(
	db *sql.DB, originalTable string, resultDB string, resultTable string,
) error {
	s.Lock()
	defer s.Unlock()

	originalTableLower := strings.ToLower(originalTable)
	resultDBLower := strings.ToLower(resultDB)
	resultTableLower := strings.ToLower(resultTable)

	sink, err := CreateSink(db, originalTableLower, resultDBLower, resultTableLower)
	if err != nil {
		return err
	}

	s.sinks[originalTableLower] = sink
	return nil
}

// FindSink returns a sink for a given table name.
func (s *Sinks) FindSink(db *sql.DB, table string) *Sink {
	s.RLock()
	defer s.RUnlock()
	result, _ := s.sinks[table]
	return result
}

// HandleResolvedRequest parses and applies all the resolved upserts.
func (s *Sinks) HandleResolvedRequest(
	db *sql.DB, rURL resolvedURL, w http.ResponseWriter, r *http.Request,
) {
	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()
	for scanner.Scan() {
		resolvedLine, err := parseResolvedLine(scanner.Bytes(), rURL.endpoint)
		if err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		log.Printf("ResolvedLine: %+v", resolvedLine)
	}
}
