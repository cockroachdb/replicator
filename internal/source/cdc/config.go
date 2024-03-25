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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

const (
	defaultBackfillWindow  = time.Hour
	defaultNDJsonBuffer    = bufio.MaxScanTokenSize // 64k
	defaultResponseTimeout = 2 * time.Minute
)

// Config adds CDC-specific configuration to the core logical loop.
type Config struct {
	DLQConfig       dlq.Config
	SequencerConfig sequencer.Config
	ScriptConfig    script.Config

	// Switch between BestEffort mode and Core if the applied resolved
	// timestamp is older than this threshold. A negative or zero value
	// will disable BestEffort switching.
	BestEffortWindow time.Duration

	// Force the use of BestEffort mode.
	BestEffortOnly bool

	// Discard all incoming HTTP payloads. This is useful for tuning
	// changefeed throughput without considering cdc-sink performance.
	Discard bool

	// If non-zero, wait half before and after consuming the payload.
	DiscardDelay time.Duration

	// Write directly to staging tables. May limit compatibility with
	// schemas that contain foreign keys.
	Immediate bool

	// The maximum amount of data to buffer when reading a single line
	// of ndjson input. This can be increased if the source cluster
	// has large blob values.
	NDJsonBuffer int

	// The maximum amount of time that we want to allow an HTTP handler
	// to run for.
	ResponseTimeout time.Duration
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQConfig.Bind(f)
	c.SequencerConfig.Bind(f)
	c.ScriptConfig.Bind(f)

	f.DurationVar(&c.BestEffortWindow, "bestEffortWindow", defaultBackfillWindow,
		"use an eventually-consistent mode for initial backfill or when replication "+
			"is behind; 0 to disable")
	f.BoolVar(&c.BestEffortOnly, "bestEffortOnly", false,
		"eventually-consistent mode; useful for high throughput, skew-tolerant schemas with FKs")
	f.BoolVar(&c.Discard, "discard", false,
		"(dangerous) discard all incoming HTTP requests; useful for changefeed throughput testing")
	f.DurationVar(&c.DiscardDelay, "discardDelay", 0,
		"adds additional delay in discard mode; useful for gauging the impact of changefeed RTT")
	f.BoolVar(&c.Immediate, "immediate", false,
		"bypass staging tables and write directly to target; "+
			"recommended only for KV-style workloads with no FKs")
	f.IntVar(&c.NDJsonBuffer, "ndjsonBufferSize", defaultNDJsonBuffer,
		"the maximum amount of data to buffer while reading a single line of ndjson input; "+
			"increase when source cluster has large blob values")
	f.DurationVar(&c.ResponseTimeout, "httpResponseTimeout", defaultResponseTimeout,
		"the maximum amount of time to allow an HTTP handler to execute for")
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

	if c.Discard {
		log.Warn("⚠️ HTTP server is discarding incoming payloads ⚠️")
	}
	if c.NDJsonBuffer == 0 {
		c.NDJsonBuffer = defaultNDJsonBuffer
	}
	if c.ResponseTimeout == 0 {
		c.ResponseTimeout = defaultResponseTimeout
	}

	return nil
}
