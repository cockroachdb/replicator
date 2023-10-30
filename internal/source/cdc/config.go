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
	defaultBackupPolling         = 100 * time.Millisecond
	defaultIdealBatchSize        = 1000
	defaultMetaTable             = "resolved_timestamps"
	defaultLargeTransactionLimit = 0                      // Opt into reduced consistency.
	defaultNDJsonBuffer          = bufio.MaxScanTokenSize // 64k
	defaultTimestampWindowSize   = 1000
)

// Config adds CDC-specific configuration to the core logical loop.
type Config struct {
	logical.BaseConfig

	// A polling loop is necessary when cdc-sink is deployed as a
	// replicated network service. There's no guarantee that the
	// instance of cdc-sink that holds the lease for resolving
	// timestamps will receive the incoming resolved-timestamp message.
	BackupPolling time.Duration

	// If true, the resolver loop will behave as though
	// TimestampWindowSize has been set to 1.  That is, each unique
	// timestamp within a resolved-timestamp window will be processed,
	// instead of fast-forwarding to the latest values within a batch.
	// This flag is relevant for use-cases that employ a merge function
	// which must see every incremental update in order to produce a
	// meaningful result.
	FlushEveryTimestamp bool

	// This setting controls a soft maximum on the number of rows that
	// will be flushed in one target transaction.
	IdealFlushBatchSize int

	// If non-zero and the number of rows in a single timestamp exceeds
	// this value, allow that source transaction to be applied over
	// multiple target transactions.
	LargeTransactionLimit int

	// The name of the resolved_timestamps table.
	MetaTableName ident.Ident

	// The maximum amount of data to buffer when reading a single line
	// of ndjson input. This can be increased if the source cluster
	// has large blob values.
	NDJsonBuffer int

	// Retain staged, applied data for an extra amount of time. This
	// allows, for example, additional time to validate what was staged
	// versus what was applied. When set to zero, staged mutations may
	// be retired as soon as there is a resolved timestamp greater than
	// the timestamp of the mutation.
	RetireOffset time.Duration

	// The maximum number of source transactions to unstage at once.
	// This does not place a hard limit on the number of mutations that
	// may be dequeued at once, but it does reduce the total number of
	// round-trips to the staging database when the average transaction
	// size is small.
	TimestampWindowSize int
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.BaseConfig.Bind(f)

	f.DurationVar(&c.BackupPolling, "backupPolling", defaultBackupPolling,
		"poll for resolved timestamps from other instances of cdc-sink")
	f.BoolVar(&c.FlushEveryTimestamp, "flushEveryTimestamp", false,
		"don't fast-forward to the latest row values within a resolved timestamp; "+
			"may negatively impact throughput")
	f.IntVar(&c.IdealFlushBatchSize, "idealFlushBatchSize", defaultIdealBatchSize,
		"a soft limit on the number of rows to send to the target at once")
	f.IntVar(&c.LargeTransactionLimit, "largeTransactionLimit", defaultLargeTransactionLimit,
		"if non-zero, all source transactions with more than this "+
			"number of rows may be applied in multiple target transactions")
	f.IntVar(&c.NDJsonBuffer, "ndjsonBufferSize", defaultNDJsonBuffer,
		"the maximum amount of data to buffer while reading a single line of ndjson input; "+
			"increase when source cluster has large blob values")
	f.Var(ident.NewValue(defaultMetaTable, &c.MetaTableName), "metaTable",
		"the name of the table in which to store resolved timestamps")
	f.DurationVar(&c.RetireOffset, "retireOffset", 0,
		"if non-zero, retain staged, applied data for an extra duration")
	f.IntVar(&c.TimestampWindowSize, "timestampWindowSize", defaultTimestampWindowSize,
		"the maximum number of source transaction timestamps to unstage at once")

	var deprecated int
	const msg = "replaced by --largeTransactionLimit and --timestampWindowSize"
	f.IntVar(&deprecated, "selectBatchSize", 0, "")
	_ = f.MarkDeprecated("selectBatchSize", msg)
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
		c.IdealFlushBatchSize = defaultIdealBatchSize
	}
	// Zero is legal; implies no limit.
	if c.LargeTransactionLimit < 0 {
		return errors.New("largeTransactionLimit must be >= 0")
	}
	if c.NDJsonBuffer == 0 {
		c.NDJsonBuffer = defaultNDJsonBuffer
	}
	if c.MetaTableName.Empty() {
		c.MetaTableName = ident.New(defaultMetaTable)
	}
	if c.RetireOffset < 0 {
		return errors.New("retireOffset must be >= 0")
	}
	if c.TimestampWindowSize < 0 {
		return errors.New("timestampWindowSize must be >= 0")
	} else if c.TimestampWindowSize == 0 {
		c.TimestampWindowSize = defaultTimestampWindowSize
	}

	return nil
}
