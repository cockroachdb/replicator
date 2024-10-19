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

package pglogical

import (
	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	scriptRT "github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/immediate"
	"github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/target/schemawatch"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConn,
	ProvideEagerConfig,
	ProvideSchemaWatchConfig,
)

// ProvideConn is called by Wire to construct a connection to the source
// database. This provider will perform some pre-flight tests on the
// source database to ensure that replication has been configured.
// There's a fake dependency on the script loader so that flags can be
// evaluated first.
func ProvideConn(
	ctx *stopper.Context,
	acc *apply.Acceptor,
	chaos *chaos.Chaos,
	config *Config,
	imm *immediate.Immediate,
	memo types.Memo,
	scriptSeq *script.Sequencer,
	stagingPool *types.StagingPool,
	targetPool *types.TargetPool,
	watchers types.Watchers,
) (*Conn, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	// Verify that the publication and replication slots were configured
	// by the user. We could create the replication slot ourselves, but
	// we want to coordinate the timing of the backup, restore, and
	// streaming operations.
	source, err := stdpool.OpenPgxAsConn(ctx, config.SourceConn)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to source database")
	}

	// Ensure that the requested publication exists.
	var count int
	if err := source.QueryRow(ctx,
		"SELECT count(*) FROM pg_publication WHERE pubname = $1",
		config.Publication,
	).Scan(&count); err != nil {
		return nil, errors.WithStack(err)
	}
	if count != 1 {
		return nil, errors.Errorf(
			`run CREATE PUBLICATION %s FOR ALL TABLES; in source database`,
			config.Publication)
	}
	log.Tracef("validated that publication %q exists", config.Publication)

	// Verify that the consumer slot exists.
	if err := source.QueryRow(ctx,
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
		config.Slot,
	).Scan(&count); err != nil {
		return nil, errors.WithStack(err)
	}
	if count != 1 {
		return nil, errors.Errorf(
			"run SELECT pg_create_logical_replication_slot('%s', 'pgoutput'); in source database, "+
				"then perform bulk data copy",
			config.Slot)
	}
	log.Tracef("validated that replication slot %q exists", config.Slot)

	// Copy the configuration and tweak it for replication behavior.
	sourceConfig := source.Config().Config.Copy()
	sourceConfig.RuntimeParams["replication"] = "database"

	seq, err := scriptSeq.Wrap(ctx, imm)
	if err != nil {
		return nil, err
	}
	seq, err = chaos.Wrap(ctx, seq) // No-op if probability is 0.
	if err != nil {
		return nil, err
	}
	connAcceptor, statVar, err := seq.Start(ctx, &sequencer.StartOptions{
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

	conn := &Conn{
		acceptor:        connAcceptor,
		columns:         &ident.TableMap[[]types.ColData]{},
		memo:            memo,
		publicationName: config.Publication,
		relations:       make(map[uint32]ident.Table),
		slotName:        config.Slot,
		sourceConfig:    sourceConfig,
		standbyTimeout:  config.StandbyTimeout,
		stagingDB:       stagingPool,
		stat:            statVar,
		target:          config.TargetSchema,
		targetDB:        targetPool,
	}
	return conn, conn.Start(ctx)
}

// ProvideEagerConfig is a hack to move up the evaluation of the user
// script so that the options callbacks can set any non-script-related
// CLI flags. The configuration will be preflighted.
func ProvideEagerConfig(cfg *Config, _ *scriptRT.Loader) (*EagerConfig, error) {
	return (*EagerConfig)(cfg), cfg.Preflight()
}

// ProvideSchemaWatchConfig is called by Wire.
func ProvideSchemaWatchConfig(cfg *Config) *schemawatch.Config {
	return &cfg.SchemaWatchConfig
}
