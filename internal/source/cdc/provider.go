// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	wire.Struct(new(Handler), "*"), // Handler is itself trivial.
	ProvideMetaTable,
	ProvideResolvers,
)

// MetaTable is an injectable configuration point.
type MetaTable ident.Table

// Table returns the underlying table identifier.
func (t MetaTable) Table() ident.Table { return ident.Table(t) }

// ProvideMetaTable is called by wire. It returns the
// "_cdc_sink.public.resolved_timestamps" table per the flags.
func ProvideMetaTable(cfg *Config) MetaTable {
	return MetaTable(ident.NewTable(cfg.StagingDB, ident.Public, cfg.MetaTableName))
}

// ProvideResolvers is called by Wire.
func ProvideResolvers(
	ctx context.Context,
	cfg *Config,
	metaTable MetaTable,
	pool *pgxpool.Pool,
	stagers types.Stagers,
	watchers types.Watchers,
) (*Resolvers, func(), error) {
	if _, err := pool.Exec(ctx, fmt.Sprintf(schema, metaTable.Table())); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	ret := &Resolvers{
		cfg:       cfg,
		metaTable: metaTable.Table(),
		pool:      pool,
		stagers:   stagers,
		watchers:  watchers,
	}
	ret.mu.instances = make(map[ident.Schema]*resolver)

	// Resume from previous state.
	schemas, err := ScanForTargetSchemas(ctx, pool, ret.metaTable)
	if err != nil {
		return nil, nil, err
	}
	for _, schema := range schemas {
		if _, err := ret.get(ctx, schema); err != nil {
			return nil, nil, errors.Wrapf(err, "could not bootstrap resolver for schema %s", schema)
		}
	}

	return ret, ret.close, nil
}
