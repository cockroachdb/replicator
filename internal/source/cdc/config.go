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

const defaultIdealMinBatchSize = 1000

// Config adds CDC-specific configuration to the core logical loop.
type Config struct {
	logical.BaseConfig

	// Coalesce timestamps within a resolved-timestamp window until
	// at least this many mutations have been collected.
	IdealMinBatchSize int

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

	f.IntVar(&c.IdealMinBatchSize, "idealMinBatchSize", defaultIdealMinBatchSize,
		"try to apply at least this many mutations per resolved-timestamp window")
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

	if c.IdealMinBatchSize == 0 {
		c.IdealMinBatchSize = defaultIdealMinBatchSize
	}
	if c.MetaTableName.IsEmpty() {
		return errors.New("no metadata table specified")
	}

	return nil
}
