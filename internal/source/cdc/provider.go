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
	"github.com/cockroachdb/cdc-sink/internal/conveyor"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/retire"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/switcher"
	"github.com/cockroachdb/cdc-sink/internal/staging/checkpoint"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/dlq"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	wire.Struct(new(Handler), "*"), // Handler is itself trivial.
	ProvideDLQConfig,
	ProvideScriptConfig,
	ProvideSequencerConfig,
	ProvideConveyors,
)

// ProvideDLQConfig is called by Wire.
func ProvideDLQConfig(cfg *Config) *dlq.Config {
	return &cfg.DLQConfig
}

// ProvideScriptConfig is called by Wire.
func ProvideScriptConfig(cfg *Config) *script.Config {
	return &cfg.ScriptConfig
}

// ProvideSequencerConfig is called by Wire.
func ProvideSequencerConfig(cfg *Config) *sequencer.Config {
	return &cfg.SequencerConfig
}

// ProvideConveyors is called by Wire.
func ProvideConveyors(
	ctx *stopper.Context,
	acc *apply.Acceptor,
	cfg *Config,
	checkpoints *checkpoint.Checkpoints,
	retire *retire.Retire,
	staging *types.StagingPool,
	sw *switcher.Switcher,
	watchers types.Watchers,
) (*conveyor.Conveyors, error) {
	if err := cfg.Preflight(); err != nil {
		return nil, err
	}
	conveyors := &conveyor.Conveyors{
		Cfg:           &cfg.ConveyorConfig,
		Checkpoints:   checkpoints,
		Kind:          "cdc",
		Retire:        retire,
		Stopper:       ctx,
		Switcher:      sw,
		TableAcceptor: acc,
		Watchers:      watchers,
	}

	// Bootstrap existing schemas for recovery cases.
	schemas, err := checkpoints.ScanForTargetSchemas(ctx)
	if err != nil {
		return nil, err
	}
	for _, sch := range schemas {
		_, err := conveyors.Get(sch)
		if err != nil {
			return nil, err
		}
	}

	return conveyors, nil
}
