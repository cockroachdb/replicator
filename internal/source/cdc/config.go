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

	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/target/schemawatch"
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
	ConveyorConfig    conveyor.Config
	DLQConfig         dlq.Config
	SequencerConfig   sequencer.Config
	ScriptConfig      script.Config
	SchemaWatchConfig schemawatch.Config
	// Discard all incoming HTTP payloads. This is useful for tuning
	// changefeed throughput without considering Replicator performance.
	Discard bool

	// If non-zero, wait half before and after consuming the payload.
	DiscardDelay time.Duration

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
	c.ConveyorConfig.Bind(f)
	c.DLQConfig.Bind(f)
	c.SequencerConfig.Bind(f)
	c.ScriptConfig.Bind(f)
	c.SchemaWatchConfig.Bind(f)

	f.BoolVar(&c.Discard, "discard", false,
		"(dangerous) discard all incoming HTTP requests; useful for changefeed throughput testing")
	f.DurationVar(&c.DiscardDelay, "discardDelay", 0,
		"adds additional delay in discard mode; useful for gauging the impact of changefeed RTT")
	f.IntVar(&c.NDJsonBuffer, "ndjsonBufferSize", defaultNDJsonBuffer,
		"the maximum amount of data to buffer while reading a single line of ndjson input; "+
			"increase when source cluster has large blob values")
	f.DurationVar(&c.ResponseTimeout, "httpResponseTimeout", defaultResponseTimeout,
		"the maximum amount of time to allow an HTTP handler to execute for")
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	if err := c.ConveyorConfig.Preflight(); err != nil {
		return err
	}
	if err := c.DLQConfig.Preflight(); err != nil {
		return err
	}
	if err := c.SequencerConfig.Preflight(); err != nil {
		return err
	}
	if err := c.ScriptConfig.Preflight(); err != nil {
		return err
	}
	if err := c.SchemaWatchConfig.Preflight(); err != nil {
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
