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
	"context"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideDialect,
	ProvideLoop,
)

// ProvideDialect is called by Wire to construct this package's
// logical.Dialect implementation. This provider will perform some
// pre-flight tests on the source database to ensure that replication
// has been configured. There's a fake dependency on the script loader
// so that flags can be evaluated first.
func ProvideDialect(
	ctx context.Context, config *Config, _ *script.Loader,
) (logical.Dialect, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	// Verify that the publication and replication slots were configured
	// by the user. We could create the replication slot ourselves, but
	// we want to coordinate the timing of the backup, restore, and
	// streaming operations.
	source, cleanup, err := stdpool.OpenPgxAsConn(ctx, config.SourceConn)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to source database")
	}
	// We dial again when the logical loop asks us to run.
	defer cleanup()

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

	return &conn{
		columns:         &ident.TableMap[[]types.ColData]{},
		publicationName: config.Publication,
		relations:       make(map[uint32]ident.Table),
		slotName:        config.Slot,
		sourceConfig:    sourceConfig,
	}, nil
}

// ProvideLoop is called by Wire to construct the sole logical loop used
// in the pglogical mode.
func ProvideLoop(
	cfg *Config, dialect logical.Dialect, loops *logical.Factory,
) (*logical.Loop, func(), error) {
	cfg.Dialect = dialect
	return loops.Start(&cfg.LoopConfig)
}
