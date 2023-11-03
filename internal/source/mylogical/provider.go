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

package mylogical

import (
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideDialect,
	ProvideLoop,
)

// ProvideDialect is called by Wire to construct this package's
// logical.Dialect implementation. There's a fake dependency on
// the script loader so that flags can be evaluated first.
func ProvideDialect(config *Config, _ *script.Loader) (logical.Dialect, error) {
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
		config:       config,
		columns:      &ident.TableMap[[]types.ColData]{},
		flavor:       flavor,
		relations:    make(map[uint64]ident.Table),
		sourceConfig: cfg,
	}, nil
}

// ProvideLoop is called by Wire to construct the sole logical loop used
// in the mylogical mode.
func ProvideLoop(
	cfg *Config, dialect logical.Dialect, loops *logical.Factory,
) (*logical.Loop, error) {
	cfg.Dialect = dialect
	return loops.Start(&cfg.LoopConfig)
}
