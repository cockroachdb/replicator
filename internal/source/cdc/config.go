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

package cdc

import (
	"bufio"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/target/dlq"
	"github.com/spf13/pflag"
)

const (
	defaultBackfillWindow = time.Hour
	defaultNDJsonBuffer   = bufio.MaxScanTokenSize // 64k
)

// Config adds CDC-specific configuration to the core logical loop.
type Config struct {
	DLQConfig       dlq.Config
	SequencerConfig sequencer.Config
	ScriptConfig    script.Config

	// Switch between BestEffort mode and Serial or Shingle if the
	// applied resolved timestamp is older than this threshold. A
	// negative or zero value will disable BestEffort switching.
	BackfillWindow time.Duration

	// Force the use of BestEffort mode.
	BestEffortOnly bool

	// Write directly to staging tables. May limit compatibility with
	// schemas that contain foreign keys.
	Immediate bool

	// The maximum amount of data to buffer when reading a single line
	// of ndjson input. This can be increased if the source cluster
	// has large blob values.
	NDJsonBuffer int

	// Enable concurrent application of transactions with
	// non-overlapping keys. This replaces the use of a purely serial
	// mode.
	Shingle bool
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQConfig.Bind(f)
	c.SequencerConfig.Bind(f)
	c.ScriptConfig.Bind(f)

	f.DurationVar(&c.BackfillWindow, "backfillWindow", defaultBackfillWindow,
		"use an eventually-consistent mode for backfill or when replication "+
			"is behind; 0 to disable")
	f.BoolVar(&c.BestEffortOnly, "bestEffortOnly", false,
		"disable serial mode; useful for high throughput, skew-tolerant schemas")
	f.BoolVar(&c.Immediate, "immediate", false,
		"bypass staging tables and write only to target; "+
			"recommended only for KV-style workloads")
	f.IntVar(&c.NDJsonBuffer, "ndjsonBufferSize", defaultNDJsonBuffer,
		"the maximum amount of data to buffer while reading a single line of ndjson input; "+
			"increase when source cluster has large blob values")
	f.BoolVar(&c.Shingle, "shingle", false,
		"enable concurrent application of transactions with non-overlapping keys; "+
			"may slightly reduce overall workload consistency")
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	if err := c.DLQConfig.Preflight(); err != nil {
		return err
	}
	if err := c.SequencerConfig.Preflight(); err != nil {
		return err
	}
	if err := c.ScriptConfig.Preflight(); err != nil {
		return err
	}

	// Backfill mode may be zero to disable BestEffort.

	if c.NDJsonBuffer == 0 {
		c.NDJsonBuffer = defaultNDJsonBuffer
	}

	return nil
}
