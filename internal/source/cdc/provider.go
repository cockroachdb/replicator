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
	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/target/schemawatch"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideHandler,
	ProvideConveyorConfig,
	ProvideDLQConfig,
	ProvideSchemaWatchConfig,
	ProvideScriptConfig,
	ProvideSequencerConfig,
	conveyor.Set,
)

// ProvideConveyorConfig is called by Wire.
func ProvideConveyorConfig(cfg *Config) *conveyor.Config {
	return &cfg.ConveyorConfig
}

// ProvideDLQConfig is called by Wire.
func ProvideDLQConfig(cfg *Config) *dlq.Config {
	return &cfg.DLQConfig
}

// ProvideSchemaWatchConfig is called by Wire.
func ProvideSchemaWatchConfig(cfg *Config) *schemawatch.Config {
	return &cfg.SchemaWatchConfig
}

// ProvideScriptConfig is called by Wire.
func ProvideScriptConfig(cfg *Config) *script.Config {
	return &cfg.ScriptConfig
}

// ProvideSequencerConfig is called by Wire.
func ProvideSequencerConfig(cfg *Config) *sequencer.Config {
	return &cfg.SequencerConfig
}

// ProvideHandler is called by Wire.
func ProvideHandler(
	auth types.Authenticator, cfg *Config, conv *conveyor.Conveyors, pool *types.TargetPool,
) (*Handler, error) {
	conveyors := conv.WithKind("cdc")
	if err := conveyors.Bootstrap(); err != nil {
		return nil, err
	}
	parser, err := cdcjson.New(cfg.NDJsonBuffer)
	if err != nil {
		return nil, err
	}
	return &Handler{
		Authenticator: auth,
		Conveyors:     conveyors,
		Config:        cfg,
		NDJsonParser:  parser,
		TargetPool:    pool,
	}, nil
}
