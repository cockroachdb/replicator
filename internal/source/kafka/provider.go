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
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/switcher"
	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/notify"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConn,
	ProvideEagerConfig,
)

// ProvideConn is called by Wire to construct this package's
// logical.Dialect implementation. There's a fake dependency on
// the script loader so that flags can be evaluated first.
func ProvideConn(
	ctx *stopper.Context,
	acc *apply.Acceptor,
	sw *switcher.Switcher,
	chaos *chaos.Chaos,
	config *Config,
	memo types.Memo,
	stagingPool *types.StagingPool,
	targetPool *types.TargetPool,
	watchers types.Watchers,
) (*Conn, error) {
	if err := config.Preflight(ctx); err != nil {
		return nil, err
	}
	// ModeImmediate is the only mode supported for now.
	mode := notify.VarOf(switcher.ModeImmediate)
	sw = sw.WithMode(mode)
	seq, err := chaos.Wrap(ctx, sw) // No-op if probability is 0.
	if err != nil {
		return nil, err
	}
	connAcceptor, _, err := seq.Start(ctx, &sequencer.StartOptions{
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

	ret := &Conn{
		acceptor: connAcceptor,
		config:   config,
		mode:     mode,
		targetDB: targetPool,
		watchers: watchers,
	}

	return (*Conn)(ret), ret.Start(ctx)
}

// ProvideEagerConfig is a hack to move up the evaluation of the user
// script so that the options callbacks can set any non-script-related
// CLI flags.
func ProvideEagerConfig(cfg *Config, _ *script.Loader) *EagerConfig {
	return (*EagerConfig)(cfg)
}
