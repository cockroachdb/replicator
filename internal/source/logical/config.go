// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package logical

import (
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	defaultApplyTimeout    = 30 * time.Second
	defaultBackfillWindow  = 10 * time.Minute
	defaultFanShards       = 16
	defaultRetryDelay      = 10 * time.Second
	defaultStandbyTimeout  = 5 * time.Second
	defaultTargetCacheSize = 128 // Statements may have a non-trivial cost in the db.
	defaultTargetDBConns   = 1024
	defaultBytesInFlight   = 10 * 1024 * 1024
)

// Config is implemented by dialects. This interface exists to allow coordination of
// error-checking in the Preflight method.
type Config interface {
	// Base returns the configuration.
	Base() *BaseConfig
	// Preflight validates the Config. Dialect implementations should
	// delegate to the BaseConfig's Preflight method.
	Preflight() error
}

// BaseConfig defines common configuration for all loops started by a Factory.
type BaseConfig struct {
	// The maximum length of time to wait for an incoming transaction
	// to settle (i.e. to detect stalls in the target database).
	ApplyTimeout time.Duration
	// BackfillWindow enables the use of fan mode for backfilling data
	// sources if the consistent point is older than the specified
	// duration. A zero value disables the use of backfill mode.
	BackfillWindow time.Duration
	// The maximum number of raw tuple-data that has yet to be applied
	// to the target database. This will act as an approximate upper
	// bound on the amount of in-memory tuple data by pausing the
	// replication receiver until sufficient number of other mutations
	// have been applied.
	BytesInFlight int
	// Used in testing to inject errors during processing.
	ChaosProb float32
	// The number of concurrent connections to use when writing data in
	// fan mode.
	FanShards int
	// Support ordering updates to satisfy foreign key constraints.
	ForeignKeysEnabled bool
	// Place the configuration into immediate mode, where mutations are
	// applied without waiting for transaction boundaries.
	Immediate bool
	// The amount of time to sleep between replication-loop retries.
	// If zero, a default value will be used.
	RetryDelay time.Duration
	// Userscript configuration.
	//
	// TODO(bob): Should this be moved to LoopConfig?
	ScriptConfig script.Config
	// How often to commit the latest consistent point.
	StandbyTimeout time.Duration
	// Connection stsring for the staging cluster.
	StagingConn string
	// The name of a SQL schema in the staging cluster to store
	// metadata in.
	StagingSchema ident.Schema
	// Connection string for the target cluster.
	TargetConn string
	// The number of connections to the target database. If zero, a
	// default value will be used.
	TargetDBConns int
	// The number of prepared statements to retain in the target
	// database connection pool. Depending on the database in question,
	// there may be more or fewer available resources to retain
	// statements.
	TargetStatementCacheSize int
}

// Base returns the BaseConfig.
func (c *BaseConfig) Base() *BaseConfig {
	return c
}

// Bind adds flags to the set.
func (c *BaseConfig) Bind(f *pflag.FlagSet) {
	c.ScriptConfig.Bind(f)

	f.DurationVar(&c.ApplyTimeout, "applyTimeout", defaultApplyTimeout,
		"the maximum amount of time to wait for an update to be applied")
	f.DurationVar(&c.BackfillWindow, "backfillWindow", defaultBackfillWindow,
		"use a high-throughput, but non-transactional mode if replication is this far behind (0 disables this feature)")
	f.IntVar(&c.BytesInFlight, "bytesInFlight", defaultBytesInFlight,
		"apply backpressure when amount of in-flight mutation data reaches this limit")
	f.BoolVar(&c.Immediate, "immediate", false,
		"apply data without waiting for transaction boundaries")
	f.IntVar(&c.FanShards, "fanShards", defaultFanShards,
		"the number of concurrent connections to use when writing data in fan mode")
	f.BoolVar(&c.ForeignKeysEnabled, "foreignKeys", false,
		"re-order updates to satisfy foreign key constraints")
	f.DurationVar(&c.RetryDelay, "retryDelay", defaultRetryDelay,
		"the amount of time to sleep between replication retries")
	c.StagingSchema = ident.MustSchema(ident.New("_cdc_sink"), ident.Public)
	// stagingDB is deprecated.
	f.Var(ident.NewSchemaFlag(&c.StagingSchema), "stagingDB",
		"a SQL database schema to store metadata in")
	// Only returns an error if flag can't be found.
	if err := f.MarkDeprecated("stagingDB", "use --stagingSchema instead"); err != nil {
		panic(err)
	}
	f.Var(ident.NewSchemaFlag(&c.StagingSchema), "stagingSchema",
		"a SQL database schema to store metadata in")
	f.DurationVar(&c.StandbyTimeout, "standbyTimeout", defaultStandbyTimeout,
		"how often to commit the consistent point")
	f.StringVar(&c.StagingConn, "stagingConn", "",
		"the staging CockroachDB cluster's connection string; required if target is other than CRDB")
	f.StringVar(&c.TargetConn, "targetConn", "",
		"the target database's connection string; always required")
	f.IntVar(&c.TargetDBConns, "targetDBConns", defaultTargetDBConns,
		"the maximum pool size to the target cluster")
	f.IntVar(&c.TargetStatementCacheSize, "targetStatementCacheSize", defaultTargetCacheSize,
		"the maximum number of prepared statements to retain")
}

// Copy returns a deep copy of the Config.
func (c *BaseConfig) Copy() *BaseConfig {
	ret := *c
	return &ret
}

// Preflight ensures that unset configuration options have sane defaults
// and returns an error if the Config is missing any fields for which a
// default cannot be provided.
func (c *BaseConfig) Preflight() error {
	if err := c.ScriptConfig.Preflight(); err != nil {
		return err
	}

	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = defaultApplyTimeout
	}
	if c.BackfillWindow < 0 {
		return errors.New("backfillWindow must be >= 0")
	}
	if c.BytesInFlight == 0 {
		c.BytesInFlight = defaultBytesInFlight
	}
	if c.FanShards == 0 {
		c.FanShards = defaultFanShards
	}
	if c.ForeignKeysEnabled && c.Immediate {
		return errors.New("foreign-key mode incompatible with immediate mode")
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = defaultRetryDelay
	}
	if c.StagingSchema.Empty() {
		return errors.New("no staging database specified")
	}
	if c.StandbyTimeout == 0 {
		c.StandbyTimeout = defaultStandbyTimeout
	}
	if c.StagingConn == "" {
		c.StagingConn = c.TargetConn // TargetConn is tested below.
	}
	if c.TargetConn == "" {
		return errors.New("targetConn must be set")
	}
	if c.TargetDBConns == 0 {
		c.TargetDBConns = defaultTargetDBConns
	}
	if c.TargetStatementCacheSize == 0 {
		c.TargetStatementCacheSize = defaultTargetCacheSize
	}
	return nil
}

// LoopConfig applies to a singular instance of a logical replication
// loop. Depending on the deployment model, there may be exactly one or
// multiple loops operating concurrently.
type LoopConfig struct {
	// The default Consistent Point to use for replication. Consistent
	// Point persisted in the target database will be used, if
	// available.
	//
	// TODO(bob): Can this field be eliminated if the Dialect's
	// ZeroStamp returns this default value instead?
	DefaultConsistentPoint string
	// The instance of the Dialect to send events to.
	Dialect Dialect
	// Uniquely identifies the replication loop.
	LoopName string
	// The SQL schema in the target cluster to write into. This value is
	// optional if a userscript dispatch function is present.
	TargetSchema ident.Schema
	// For testing; the loop will backfill and then wait for this
	// channel to be closed before switching to incremental operation.
	// This allows tests to validate the backfill behavior.
	WaitAfterBackfill <-chan struct{} `json:"-"`
}

// Bind adds flags to the set. This method only makes sense for
// deployment scenarios where the is exactly one replication loop per
// instance of the application.
func (c *LoopConfig) Bind(f *pflag.FlagSet) {
	// Allow specializations to set the default name before binding.
	f.StringVar(&c.LoopName, "loopName", c.LoopName, "identify the replication loop in metrics")

	// DefaultConsistentPoint bound by dialect packages, since the name
	// of the flag will vary based on the product in question.

	// targetDB is deprecated.
	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetDB",
		"the SQL database schema in the target cluster to update")
	// Only returns an error if flag can't be found.
	if err := f.MarkDeprecated("targetDB", "use --targetSchema instead"); err != nil {
		panic(err)
	}
	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")
}

// Copy returns a deep copy of the config.
func (c *LoopConfig) Copy() *LoopConfig {
	ret := *c
	// Propagating this behavior is undesirable.
	ret.WaitAfterBackfill = nil
	return &ret
}

// Preflight ensures that unset configuration options have sane defaults
// and returns an error if the Config is missing any fields for which a
// default cannot be provided.
func (c *LoopConfig) Preflight() error {
	// We don't check Dialect here, since it might be possible to
	// construct it only after the LoopConfig has been built.

	if c.LoopName == "" {
		return errors.New("replication loops must be named")
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target database specified")
	}
	return nil
}
