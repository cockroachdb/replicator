// Copyright 2024 The Cockroach Authors
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

package kafka

import (
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConn,
	ProvideConveyorConfig,
	ProvideEagerConfig,
)

// ProvideEagerConfig is a hack to move up the evaluation of the user
// script so that the options callbacks can set any non-script-related
// CLI flags.
func ProvideEagerConfig(cfg *Config, _ *script.Loader) *EagerConfig {
	return (*EagerConfig)(cfg)
}

// ProvideConveyorConfig is called by Wire.
func ProvideConveyorConfig(cfg *Config) *conveyor.Config {
	return &cfg.Conveyor
}

// ProvideConn is called by Wire to construct this package's
// logical.Dialect implementation. There's a fake dependency on
// the script loader so that flags can be evaluated first.
func ProvideConn(ctx *stopper.Context, config *Config, conv *conveyor.Conveyors) (*Conn, error) {
	if err := config.Preflight(ctx); err != nil {
		return nil, err
	}
	conveyors := conv.WithKind("kafka")
	if err := conveyors.Bootstrap(); err != nil {
		return nil, err
	}
	conveyor, err := conveyors.Get(config.TargetSchema)
	if err != nil {
		return nil, err
	}
	conn := &Conn{
		config:   config,
		conveyor: conveyor,
	}
	return (*Conn)(conn), conn.Start(ctx)
}
