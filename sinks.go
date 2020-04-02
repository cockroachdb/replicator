package main

import (
	"database/sql"
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
