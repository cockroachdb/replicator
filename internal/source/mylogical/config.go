// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mylogical

import (
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

const (
	defaultRetryDelay = 10 * time.Second
	defaultServerID   = 42
)

var wildcard = ident.New("*")

// Config contains the configuration necessary for creating a
// replication connection. All field, other than TestControls, are
// mandatory.
type Config struct {
	// Place the configuration into immediate mode, where mutations are
	// applied without waiting for transaction boundaries.
	Immediate bool
	// The source database's hostname or IP address.
	SourceHost string
	// The password to use when connecting to the source database.
	SourcePassword string
	// If zero, the protocol default of 3306 will be used.
	SourcePort int
	// The unique replication ID to be reported by the client.
	SourceServerID int
	// SourceTables must contain at least one entry in order to filter
	// incoming binlog entries. All tables in a database may be selected
	// by using a "*" as the table name.
	SourceTables []ident.Table
	// The username to connect to the source database as.
	SourceUser string
	// The amount of time to sleep between replication-loop retries.
	// If zero, a default value will be used.
	RetryDelay time.Duration
	// Connection string for the target cluster.
	TargetConn string
	// The SQL database in the target cluster to write into.
	TargetDB ident.Ident
	// Additional controls for testing.
	TestControls *TestControls
}

// Copy returns a deep copy of the Config.
func (c *Config) Copy() *Config {
	cpy := *c
	cpy.SourceTables = make([]ident.Table, len(c.SourceTables))
	copy(cpy.SourceTables, c.SourceTables)
	return &cpy
}

// preflight returns  an error if a critical field is missing data.
func (c *Config) preflight() error {
	if c.SourceHost == "" {
		return errors.New("no source host specified")
	}
	if c.SourcePort == 0 {
		return errors.New("no source port specified")
	}
	if c.SourcePassword == "" {
		return errors.New("no source password specified")
	}
	if len(c.SourceTables) == 0 {
		return errors.New("no source table filters")
	}
	if c.SourceUser == "" {
		return errors.New("no source user specified")
	}
	if c.TargetConn == "" {
		return errors.New("no target connection string specified")
	}
	if c.TargetDB.IsEmpty() {
		return errors.New("no target database specified")
	}
	return nil
}

// TestControls define a collection of testing hook points for injecting
// behavior in a testing scenario. The function callbacks in this type
// must be prepared to be called from arbitrary goroutines. All fields
// in this type are optional.
type TestControls struct {
	BreakOnDataTuple     func() bool
	BreakReplicationFeed func() bool
	BreakSinkFlush       func() bool
}

func accept(filters []ident.Table, incoming ident.Table) bool {
	for _, filter := range filters {
		if filter.Database() != incoming.Database() {
			continue
		}
		// MySQL doesn't support user-defined schemas, so we ignore
		// the schema component.
		if filter.Table() == wildcard || filter.Table() == incoming.Table() {
			return true
		}
	}
	return false
}
