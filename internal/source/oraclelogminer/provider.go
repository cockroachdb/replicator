// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Package oraclelogminer contains components necessary for the oracle source frontend.
package oraclelogminer

import (
	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/core"
	scriptSeq "github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/source/oraclelogminer/scn"
	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
	"github.com/google/wire"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideDB,
	ProvideEagerConfig,
)

// ProvideDB is called by Wire to construct a connection pool to the source
// database. This provider will perform some pre-flight tests on the
// source database to ensure that replication has been configured.
// There's a fake dependency on the script loader so that flags can be
// evaluated first.
func ProvideDB(
	ctx *stopper.Context,
	acc *apply.Acceptor,
	chaos *chaos.Chaos,
	config *Config,
	coreSequencer *core.Core,
	scriptSeq *scriptSeq.Sequencer,
	targetPool *types.TargetPool,
	watchers types.Watchers,
) (*DB, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	sourcePool, err := stdpool.OpenOracleAsSource(ctx, config.SourceConn)
	if err != nil {
		return nil, err
	}
	// TODO(janexing): switch to core sequencer rather than immediate one.
	// Wait for Bob's PR.
	// Core sequencers require a replayable buffer,
	// Core can deal with assembling mutations from the staging, and can write txn concurrently
	// more robust, enhance the efficiency of apply, because it write to target async.
	// This allows reading from logminer while the core sequencer is applying mutations async.
	// Change core seq to accept chan of mut, rather than chan of batch
	seq, err := scriptSeq.Wrap(ctx, coreSequencer)
	if err != nil {
		return nil, err
	}
	seq, err = chaos.Wrap(ctx, seq) // No-op if probability is 0.
	if err != nil {
		return nil, err
	}

	// TODO(janexing): take the notify Stats and push the progress of tables into memo.

	db := &DB{
		SourcePool: sourcePool,
		targetDB:   targetPool,
		config:     config,
		walOffset:  notify.Var[*scn.SCN]{},
	}

	_, stat, err := seq.Start(ctx, &sequencer.StartOptions{
		BatchReader: db,
		Delegate:    types.OrderedAcceptorFrom(acc, watchers),
		Bounds:      &notify.Var[hlc.Range]{}, // Not currently used.
		Group: &types.TableGroup{
			Name:      ident.New(config.TargetSchema.Raw()),
			Enclosing: config.TargetSchema,
		},
	})

	if err != nil {
		return nil, err
	}

	// Sync the sequencer's progress back to walOff value for SCN.
	ctx.Go(func(ctx *stopper.Context) error {
		_, _ = stopvar.DoWhenChanged(ctx, nil, stat, func(ctx *stopper.Context, old, next sequencer.Stat) error {
			oldProgress := sequencer.CommonProgress(old).Max()
			progress := sequencer.CommonProgress(next).Max()
			// TODO(janexing): seems this condition is not reachable, figure out why.
			if hlc.Compare(progress, oldProgress) > 0 {
				cp := progress.External().(*scn.SCN)
				log.Debugf("progressed to consistent point: %s", cp)
				db.walOffset.Set(cp)
			}

			return nil
		})
		return nil
	})

	return db, err
}

// ProvideEagerConfig is a hack to move up the evaluation of the user
// script so that the options callbacks can set any non-script-related
// CLI flags. The configuration will be preflighted.
func ProvideEagerConfig(cfg *Config, _ *script.Loader) (*EagerConfig, error) {
	return (*EagerConfig)(cfg), cfg.Preflight()
}
