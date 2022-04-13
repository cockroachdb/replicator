// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

const (
	defaultApplyTimeout  = 30 * time.Second
	defaultRetryDelay    = 10 * time.Second
	defaultTargetDBConns = 1024
	defaultBytesInFlight = 10 * 1024 * 1024
)

// Config defines the core configuration required by logical.Loop.
type Config struct {
	// The maximum length of time to wait for an incoming transaction
	// to settle (i.e. to detect stalls in the target database).
	ApplyTimeout time.Duration
	// The maximum number of raw tuple-data that has yet to be applied
	// to the target database. This will act as an approximate upper
	// bound on the amount of in-memory tuple data by pausing the
	// replication receiver until sufficient number of other mutations
	// have been applied.
	BytesInFlight int
	// If present, the key used to persist consistent point identifiers.
	ConsistentPointKey string
	// The default Consistent Point to use for replication.
	// Consistent Point persisted in the target database will be used, if available.
	DefaultConsistentPoint string
	// Place the configuration into immediate mode, where mutations are
	// applied without waiting for transaction boundaries.
	Immediate bool
	// The amount of time to sleep between replication-loop retries.
	// If zero, a default value will be used.
	RetryDelay time.Duration
	// Connection string for the target cluster.
	TargetConn string
	// The SQL database in the target cluster to write into.
	TargetDB ident.Ident
	// The number of connections to the target database. If zero, a
	// default value will be used.
	TargetDBConns int
}

// Preflight ensures that unset configuration options have sane defaults
// and returns an error if the Config is missing any fields for which a
// default connot be provided.
func (c *Config) Preflight() error {
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = defaultApplyTimeout
	}
	if c.BytesInFlight == 0 {
		c.BytesInFlight = defaultBytesInFlight
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = defaultRetryDelay
	}
	if c.TargetConn == "" {
		return errors.New("no target connection string specified")
	}
	if c.TargetDB.IsEmpty() {
		return errors.New("no target database specified")
	}
	if c.TargetDBConns == 0 {
		c.TargetDBConns = defaultTargetDBConns
	}
	return nil
}
