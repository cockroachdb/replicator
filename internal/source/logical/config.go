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
	"github.com/spf13/pflag"
)

const (
	defaultApplyTimeout  = 30 * time.Second
	defaultRetryDelay    = 10 * time.Second
	defaultTargetDBConns = 1024
	defaultBytesInFlight = 10 * 1024 * 1024
)

// Config defines the core configuration required by logical.Loop.
type Config struct {
	// AllowBackfill enables the use of a non-transaction,
	// high-throughput mode for backfilling data sources.
	AllowBackfill bool
	// The maximum length of time to wait for an incoming transaction
	// to settle (i.e. to detect stalls in the target database).
	ApplyTimeout time.Duration
	// The maximum number of raw tuple-data that has yet to be applied
	// to the target database. This will act as an approximate upper
	// bound on the amount of in-memory tuple data by pausing the
	// replication receiver until sufficient number of other mutations
	// have been applied.
	BytesInFlight int
	// Used in testing to inject errors during processing.
	ChaosProb float32
	// If present, the key used to persist consistent point identifiers.
	ConsistentPointKey string
	// The default Consistent Point to use for replication.
	// Consistent Point persisted in the target database will be used, if available.
	DefaultConsistentPoint string
	// The number of concurrent connections to use when writing data in
	// fan mode.
	FanShards int
	// Place the configuration into immediate mode, where mutations are
	// applied without waiting for transaction boundaries.
	Immediate bool
	// The amount of time to sleep between replication-loop retries.
	// If zero, a default value will be used.
	RetryDelay time.Duration
	// How often to commit the latest consistent point.
	StandbyTimeout time.Duration
	// The name of a SQL database in the target cluster to store
	// metadata in.
	StagingDB ident.Ident
	// Connection string for the target cluster.
	TargetConn string
	// The SQL database in the target cluster to write into.
	TargetDB ident.Ident
	// The number of connections to the target database. If zero, a
	// default value will be used.
	TargetDBConns int

	stagingDB string // Temporary storage for StagingDB.
	targetDB  string // Temporary storage for TargetDB.
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	f.BoolVar(&c.AllowBackfill, "allowBackfill", false,
		"allow the temporary use of immediate mode when backfilling data")
	f.DurationVar(&c.ApplyTimeout, "applyTimeout", 30*time.Second,
		"the maximum amount of time to wait for an update to be applied")
	f.IntVar(&c.BytesInFlight, "bytesInFlight", 10*1024*1024,
		"apply backpressure when amount of in-flight mutation data reaches this limit")
	// ConsistentPointKey used only by mylogical.
	// DefaultConsistentPoint used only by mylogical.
	f.BoolVar(&c.Immediate, "immediate", false, "apply data without waiting for transaction boundaries")
	f.IntVar(&c.FanShards, "fanShards", 16,
		"the number of concurrent connections to use when writing data in fan mode")
	f.DurationVar(&c.RetryDelay, "retryDelay", 10*time.Second,
		"the amount of time to sleep between replication retries")
	f.StringVar(&c.stagingDB, "stagingDB", "_cdc_sink", "a SQL database to store metadata in")
	f.DurationVar(&c.StandbyTimeout, "standbyTimeout", 5*time.Second,
		"how often to commit the consistent point")
	f.StringVar(&c.TargetConn, "targetConn", "", "the target cluster's connection string")
	f.StringVar(&c.targetDB, "targetDB", "", "the SQL database in the target cluster to update")
	f.IntVar(&c.TargetDBConns, "targetDBConns", 1024, "the maximum pool size to the target cluster")
}

// Copy returns a deep copy of the Config.
func (c *Config) Copy() *Config {
	ret := *c
	return &ret
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
	if c.StagingDB.IsEmpty() {
		if c.stagingDB == "" {
			return errors.New("no staging database specified")
		}
		c.StagingDB = ident.New(c.stagingDB)
	}
	if c.TargetConn == "" {
		return errors.New("no target connection string specified")
	}
	if c.TargetDB.IsEmpty() {
		if c.targetDB == "" {
			return errors.New("no target database specified")
		}
		c.TargetDB = ident.New(c.targetDB)
	}
	if c.TargetDBConns == 0 {
		c.TargetDBConns = defaultTargetDBConns
	}
	return nil
}
