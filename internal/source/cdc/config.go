// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"bufio"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	defaultFlushBatchSize  = 1_000
	defaultSelectBatchSize = 10_000
	defaultNDJsonBuffer    = bufio.MaxScanTokenSize // 64k
)

// Config adds CDC-specific configuration to the core logical loop.
type Config struct {
	logical.BaseConfig

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
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.BaseConfig.Bind(f)

	// We set the targetDB based on the value in the incoming HTTP
	// request.
	if err := f.MarkHidden("targetDB"); err != nil {
		panic(err)
	}

	f.IntVar(&c.IdealFlushBatchSize, "idealFlushBatchSize", defaultFlushBatchSize,
		"try to apply at least this many mutations per resolved-timestamp window")
	f.IntVar(&c.NDJsonBuffer, "ndjsonBufferSize", defaultNDJsonBuffer,
		"the maximum amount of data to buffer while reading a single line of ndjson input; "+
			"increase when source cluster has large blob values")
	f.Var(ident.NewValue("resolved_timestamps", &c.MetaTableName), "metaTable",
		"the name of the table in which to store resolved timestamps")
	f.IntVar(&c.SelectBatchSize, "selectBatchSize", defaultSelectBatchSize,
		"the number of rows to select at once when reading staged data")
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	c.BaseConfig.LoopName = "changefeed"
	c.BaseConfig.TargetDB = ident.New("__filled_in_later__")

	if err := c.Base().Preflight(); err != nil {
		return err
	}

	if c.IdealFlushBatchSize == 0 {
		c.IdealFlushBatchSize = defaultFlushBatchSize
	}
	if c.NDJsonBuffer == 0 {
		c.NDJsonBuffer = defaultNDJsonBuffer
	}
	if c.MetaTableName.IsEmpty() {
		return errors.New("no metadata table specified")
	}
	if c.SelectBatchSize == 0 {
		c.SelectBatchSize = defaultSelectBatchSize
	}

	return nil
}
