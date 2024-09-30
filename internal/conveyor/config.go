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

package conveyor

import (
	"time"

	"github.com/spf13/pflag"
)

const (
	// DefaultBestEffortWindow determines the default value, set to 1
	// hour, to switch from BestEffort to Core, if the applied resolved
	// timestamp is older than this threshold.
	DefaultBestEffortWindow = time.Hour
)

// Config defines the behavior for a Conveyor.
type Config struct {
	// Switch between BestEffort mode and Core if the applied resolved
	// timestamp is older than this threshold. A negative or zero value
	// will disable BestEffort switching.
	BestEffortWindow time.Duration

	// Force the use of BestEffort mode.
	BestEffortOnly bool

	// Don't use a core changefeed for cross-Replicator notifications
	// and only use a polling strategy for detecting changes to the
	// timestamp bounds.
	DisableCheckpointStream bool

	// Write directly to staging tables. May limit compatibility with
	// schemas that contain foreign keys.
	Immediate bool

	// If non-zero, limit the number of checkpoint rows that will be
	// used to compute the resolving range. This will limit the maximum
	// amount of observable skew in the target due to blocked mutations
	// (e.g. running into a lock), but will cause replication to stall
	// if behind by this many checkpoints.
	LimitLookahead int
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	f.DurationVar(&c.BestEffortWindow, "bestEffortWindow", DefaultBestEffortWindow,
		"use an eventually-consistent mode for initial backfill or when replication "+
			"is behind; 0 to disable")
	f.BoolVar(&c.BestEffortOnly, "bestEffortOnly", false,
		"eventually-consistent mode; useful for high throughput, skew-tolerant schemas with FKs")
	f.BoolVar(&c.DisableCheckpointStream, "disableCheckpointStream", false,
		"disable cross-Replicator checkpoint notifications and rely only on polling")
	f.BoolVar(&c.Immediate, "immediate", false,
		"bypass staging tables and write directly to target; "+
			"recommended only for KV-style workloads with no FKs")
	f.IntVar(&c.LimitLookahead, "limitLookahead", 0,
		"limit number of checkpoints to be considered when computing the resolving range; "+
			"may cause replication to stall completely if older mutations cannot be applied")
}

// Preflight ensures the Config is in a known-good state.
func (c *Config) Preflight() error {
	return nil
}
