// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mylogical

import (
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideBaseConfig,
	ProvideDialect,
)

// ProvideBaseConfig is called by Wire to extract the logical.Config
// from this package's Config type.
func ProvideBaseConfig(config *Config) *logical.Config {
	return &config.Config
}

// ProvideDialect is called by Wire to construct this package's
// logical.Dialect implementation.
func ProvideDialect(config *Config) (logical.Dialect, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}

	flavor, err := getFlavor(config)
	if err != nil {
		return nil, err
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID:  config.ProcessID,
		Flavor:    flavor,
		Host:      config.host,
		Port:      config.port,
		User:      config.user,
		Password:  config.password,
		TLSConfig: config.tlsConfig,
	}
	return &conn{
		columns:      make(map[ident.Table][]types.ColData),
		flavor:       flavor,
		relations:    make(map[uint64]ident.Table),
		sourceConfig: cfg,
	}, nil
}
