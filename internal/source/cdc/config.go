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

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	defaultBackupPolling   = 100 * time.Millisecond
	defaultFlushBatchSize  = 1_000
	defaultSelectBatchSize = 10_000
	defaultNDJsonBuffer    = bufio.MaxScanTokenSize // 64k
)

// Config adds CDC-specific configuration to the core logical loop.
type Config struct {
	logical.BaseConfig

	// A polling loop is necessary when cdc-sink is deployed as a
	// replicated network service. There's no guarantee that the
	// instance of cdc-sink that holds the lease for resolving
	// timestamps will receive the incoming resolved-timestamp message.
	BackupPolling time.Duration

	// Don't coalesce updates from different source MVCC timestamps into
	// a single destination transaction. Setting this will preserve the
	// original structure of updates from the source, but may decrease
	// overall throughput by increasing the total number of target
	// database transactions.
	FlushEveryTimestamp bool

	// Coalesce timestamps within a resolved-timestamp window until
	// at least this many mutations have been collected.
	IdealFlushBatchSize int

	// The maximum amount of data to buffer when reading a single line
	// of ndjson input. This can be increased if the source cluster
	// has large blob values.
	NDJsonBuffer int

	// The name of the resolved_timestamps table.
	MetaTableName ident.Ident

	// The number of rows to retrieve when loading staged data.
	SelectBatchSize int

	// Retain staged, applied data for an extra amount of time. This
	// allows, for example, additional time to validate what was staged
	// versus what was applied. When set to zero, staged mutations may
	// be retired as soon as there is a resolved timestamp greater than
	// the timestamp of the mutation.
	RetireOffset time.Duration
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.BaseConfig.Bind(f)

	f.DurationVar(&c.BackupPolling, "backupPolling", defaultBackupPolling,
		"poll for resolved timestamps from other instances of cdc-sink")
	f.BoolVar(&c.FlushEveryTimestamp, "flushEveryTimestamp", false,
		"preserve intermediate updates from the source in transactional mode; "+
			"may negatively impact throughput")
	f.IntVar(&c.IdealFlushBatchSize, "idealFlushBatchSize", defaultFlushBatchSize,
		"try to apply at least this many mutations per resolved-timestamp window")
	f.IntVar(&c.NDJsonBuffer, "ndjsonBufferSize", defaultNDJsonBuffer,
		"the maximum amount of data to buffer while reading a single line of ndjson input; "+
			"increase when source cluster has large blob values")
	f.Var(ident.NewValue("resolved_timestamps", &c.MetaTableName), "metaTable",
		"the name of the table in which to store resolved timestamps")
	f.IntVar(&c.SelectBatchSize, "selectBatchSize", defaultSelectBatchSize,
		"the number of rows to select at once when reading staged data")
	f.DurationVar(&c.RetireOffset, "retireOffset", 0,
		"retain staged, applied data for an extra duration")
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	if err := c.BaseConfig.Preflight(); err != nil {
		return err
	}

	if c.BackupPolling == 0 {
		c.BackupPolling = defaultBackupPolling
	}
	if c.IdealFlushBatchSize == 0 {
		c.IdealFlushBatchSize = defaultFlushBatchSize
	}
	if c.NDJsonBuffer == 0 {
		c.NDJsonBuffer = defaultNDJsonBuffer
	}
	if c.MetaTableName.Empty() {
		return errors.New("no metadata table specified")
	}
	if c.SelectBatchSize == 0 {
		c.SelectBatchSize = defaultSelectBatchSize
	}
	if c.RetireOffset < 0 {
		return errors.New("retireOffset must be >= 0")
	}

	return nil
}
