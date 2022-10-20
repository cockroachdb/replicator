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
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config adds CDC-specific configuration to the core logical loop.
type Config struct {
	logical.BaseConfig

	BackfillBatchSize int

	// The name of the resolved_timestamps table.
	MetaTableName ident.Ident
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.BaseConfig.Bind(f)

	// We set the targetDB based on the value in the incoming HTTP
	// request.
	if err := f.MarkHidden("targetDB"); err != nil {
		panic(err)
	}

	f.IntVar(&c.BackfillBatchSize, "backfillBatchSize", 1_000,
		"the number of updates to process per backfill batch")
	f.Var(ident.NewValue("resolved_timestamps", &c.MetaTableName), "metaTable",
		"the name of the table in which to store resolved timestamps")
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	c.BaseConfig.LoopName = "changefeed"
	c.BaseConfig.TargetDB = ident.New("__filled_in_later__")

	if err := c.Base().Preflight(); err != nil {
		return err
	}

	if c.BackfillWindow > 0 && c.BackfillBatchSize <= 0 {
		return errors.New("backfilling enabled, but batch size is 0")
	}
	if c.MetaTableName.IsEmpty() {
		return errors.New("no metadata table specified")
	}

	return nil
}
