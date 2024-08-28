// Copyright 2024 The Cockroach Authors
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

package sequencer

import (
	"time"

	"github.com/spf13/pflag"
)

// Defaults for flag bindings.
const (
	DefaultFlushPeriod     = 1 * time.Second
	DefaultFlushSize       = 1_000
	DefaultTaskGracePeriod = time.Minute
	DefaultParallelism     = 16
	DefaultQuiescentPeriod = 10 * time.Second
	DefaultRetireOffset    = 24 * time.Hour
	DefaultScanSize        = 10_000
	DefaultTimestampLimit  = 1_000
)

// Config is an injection point common to sequencer implementations. Not
// all sequencers necessarily respond to all configuration options.
type Config struct {
	Chaos           float32       // Set by tests to inject errors.
	FlushPeriod     time.Duration // Don't queue mutations for longer than this.
	FlushSize       int           // Ideal target database transaction size
	Parallelism     int           // The number of concurrent connections to use.
	QuiescentPeriod time.Duration // How often to sweep for queued mutations.
	RetireOffset    time.Duration // Delay removal of applied mutations.
	ScanSize        int           // Limit on staging-table read queries.
	TaskGracePeriod time.Duration // How long to allow previous iteration to clean up.
	TimestampLimit  int           // The maximum number of timestamps to operate on.
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(flags *pflag.FlagSet) {
	flags.DurationVar(&c.FlushPeriod, "flushPeriod", DefaultFlushPeriod,
		"flush queued mutations after this duration")
	flags.IntVar(&c.FlushSize, "flushSize", DefaultFlushSize,
		"ideal batch size to determine when to flush mutations")
	flags.IntVar(&c.Parallelism, "parallelism", DefaultParallelism,
		"the number of concurrent database transactions to use")
	flags.DurationVar(&c.QuiescentPeriod, "quiescentPeriod", DefaultQuiescentPeriod,
		"how often to retry deferred mutations")
	flags.DurationVar(&c.RetireOffset, "retireOffset", DefaultRetireOffset,
		"delay removal of applied mutations")
	flags.IntVar(&c.ScanSize, "scanSize", DefaultScanSize,
		"the number of rows to retrieve from staging")
	flags.DurationVar(&c.TaskGracePeriod, "taskGracePeriod", DefaultTaskGracePeriod,
		"how long to allow for task cleanup when recovering from errors")
	flags.IntVar(&c.TimestampLimit, "timestampLimit", DefaultTimestampLimit,
		"the maximum number of source timestamps to coalesce into a target transaction")
}

// Preflight ensure that the configuration has sane defaults.
func (c *Config) Preflight() error {
	if c.FlushPeriod == 0 {
		c.FlushPeriod = DefaultFlushPeriod
	}
	if c.FlushSize == 0 {
		c.FlushSize = DefaultFlushSize
	}
	if c.Parallelism == 0 {
		c.Parallelism = DefaultParallelism
	}
	if c.QuiescentPeriod <= 0 {
		c.QuiescentPeriod = DefaultQuiescentPeriod
	}
	// 0 is a valid value for RetireOffset.
	if c.RetireOffset < 0 {
		c.RetireOffset = DefaultRetireOffset
	}
	if c.ScanSize == 0 {
		c.ScanSize = DefaultScanSize
	}
	if c.TaskGracePeriod <= 0 {
		c.TaskGracePeriod = DefaultTaskGracePeriod
	}
	if c.TimestampLimit <= 0 {
		c.TimestampLimit = DefaultTimestampLimit
	}
	return nil
}
