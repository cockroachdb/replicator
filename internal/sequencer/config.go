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
	DefaultParallelism     = 16
	DefaultQuiescentPeriod = 10 * time.Second
	DefaultRetireOffset    = 24 * time.Hour
	DefaultSweepLimit      = 10_000
	DefaultTimestampLimit  = 100
)

// Config is an injection point common to sequencer implementations. Not
// all sequencers necessarily respond to all configuration options.
type Config struct {
	Chaos           float32       // Set by tests to inject errors.
	Parallelism     int           // The number of concurrent connections to use.
	QuiescentPeriod time.Duration // How often to sweep for queued mutations.
	RetireOffset    time.Duration // Delay removal of applied mutations.
	SweepLimit      int           // How many mutations to retrieve in a single batch.
	TimestampLimit  int           // The maximum number of timestamps to operate on.
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(flags *pflag.FlagSet) {
	flags.IntVar(&c.Parallelism, "bestEffortParallelism", DefaultParallelism,
		"the number of concurrent database connections to use in best-effort mode")
	flags.DurationVar(&c.QuiescentPeriod, "quiescentPeriod", DefaultQuiescentPeriod,
		"how often to retry deferred mutations")
	flags.DurationVar(&c.RetireOffset, "retireOffset", DefaultRetireOffset,
		"delay removal of applied mutations")
	flags.IntVar(&c.SweepLimit, "bestEffortSweepLimit", DefaultSweepLimit,
		"how many deferred mutations to attempt to retry at once")
	flags.IntVar(&c.TimestampLimit, "serialTimestampLimit", DefaultTimestampLimit,
		"the maximum number of source timestamps to coalesce into a target transaction")

	// Compatibility with older fan_events flag.
	flags.IntVar(&c.Parallelism, "fanShards", DefaultParallelism,
		"use --bestEffortParallelism instead")
	_ = flags.MarkDeprecated("fanShards", "use --bestEffortParallelism instead")
}

// Preflight ensure that the configuration has sane defaults.
func (c *Config) Preflight() error {
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
	if c.SweepLimit <= 0 {
		c.SweepLimit = DefaultSweepLimit
	}
	if c.TimestampLimit <= 0 {
		c.TimestampLimit = DefaultTimestampLimit
	}
	return nil
}
