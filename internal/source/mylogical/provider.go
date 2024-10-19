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
	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/immediate"
	scriptSeq "github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/target/schemawatch"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConn,
	ProvideEagerConfig,
	ProvideSchemaWatchConfig,
)

// ProvideConn is called by Wire to construct this package's
// logical.Dialect implementation. There's a fake dependency on
// the script loader so that flags can be evaluated first.
func ProvideConn(
	ctx *stopper.Context,
	acc *apply.Acceptor,
	chaos *chaos.Chaos,
	config *Config,
	imm *immediate.Immediate,
	memo types.Memo,
	scriptSeq *scriptSeq.Sequencer,
	stagingPool *types.StagingPool,
	targetPool *types.TargetPool,
	watchers types.Watchers,
) (*Conn, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}

	flavor, _, err := getFlavor(config)
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

	seq, err := scriptSeq.Wrap(ctx, imm)
	if err != nil {
		return nil, err
	}
	seq, err = chaos.Wrap(ctx, seq) // No-op if probability is 0.
	if err != nil {
		return nil, err
	}
	connAcceptor, stat, err := seq.Start(ctx, &sequencer.StartOptions{
		Delegate: types.OrderedAcceptorFrom(acc, watchers),
		Bounds:   &notify.Var[hlc.Range]{}, // Not currently used.
		Group: &types.TableGroup{
			Name:      ident.New(config.TargetSchema.Raw()),
			Enclosing: config.TargetSchema,
		},
	})
	if err != nil {
		return nil, err
	}

	ret := &conn{
		acceptor:     connAcceptor,
		columns:      &ident.TableMap[[]types.ColData]{},
		config:       config,
		memo:         memo,
		flavor:       flavor,
		relations:    make(map[uint64]ident.Table),
		sourceConfig: cfg,
		stagingDB:    stagingPool,
		stat:         stat,
		target:       config.TargetSchema,
		targetDB:     targetPool,
		walOffset:    notify.Var[*consistentPoint]{},
	}

	return (*Conn)(ret), ret.Start(ctx)
}

// ProvideEagerConfig is a hack to move up the evaluation of the user
// script so that the options callbacks can set any non-script-related
// CLI flags. The configuration will be preflighted.
func ProvideEagerConfig(cfg *Config, _ *script.Loader) (*EagerConfig, error) {
	return (*EagerConfig)(cfg), cfg.Preflight()
}

// ProvideSchemaWatchConfig is called by Wire.
func ProvideSchemaWatchConfig(cfg *Config) *schemawatch.Config {
	return &cfg.SchemaWatchConfig
}
