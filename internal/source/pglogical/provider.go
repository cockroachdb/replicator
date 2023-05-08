// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pglogical

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideDialect)

// ProvideDialect is called by Wire to construct this package's
// logical.Dialect implementation. This provider will perform some
// pre-flight tests on the source database to ensure that replication
// has been configured.
func ProvideDialect(ctx context.Context, config *Config) (logical.Dialect, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}

	cfg, err := stdpool.ParseConfig(config.SourceConn)
	if err != nil {
		return nil, err
	}

	// Verify that the publication and replication slots were configured
	// by the user. We could create the replication slot ourselves, but
	// we want to coordinate the timing of the backup, restore, and
	// streaming operations.
	source, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to source database")
	}
	defer source.Close(context.Background())

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
		columns:         make(map[ident.Table][]types.ColData),
		publicationName: config.Publication,
		relations:       make(map[uint32]ident.Table),
		slotName:        config.Slot,
		sourceConfig:    sourceConfig,
	}, nil
}
