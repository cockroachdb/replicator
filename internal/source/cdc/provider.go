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
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/google/wire"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	wire.Struct(new(Handler), "*"), // Handler is itself trivial.
	ProvideImmediate,
	ProvideMetaTable,
	ProvideResolvers,
)

// MetaTable is an injectable configuration point.
type MetaTable ident.Table

// Table returns the underlying table identifier.
func (t MetaTable) Table() ident.Table { return ident.Table(t) }

// ProvideImmediate is called by wire.
func ProvideImmediate(loops *logical.Factory) (*Immediate, func(), error) {
	imm := &Immediate{loops: loops}
	return imm, imm.cleanup, nil
}

// ProvideMetaTable is called by wire. It returns the
// "_cdc_sink.public.resolved_timestamps" table per the flags.
func ProvideMetaTable(cfg *Config) MetaTable {
	return MetaTable(ident.NewTable(cfg.StagingSchema, cfg.MetaTableName))
}

// ProvideResolvers is called by Wire.
func ProvideResolvers(
	ctx *stopper.Context,
	cfg *Config,
	leases types.Leases,
	loops *logical.Factory,
	metaTable MetaTable,
	pool *types.StagingPool,
	stagers types.Stagers,
	watchers types.Watchers,
) (*Resolvers, error) {
	if _, err := pool.Exec(ctx, fmt.Sprintf(schema, metaTable.Table())); err != nil {
		return nil, errors.WithStack(err)
	}

	ret := &Resolvers{
		cfg:       cfg,
		leases:    leases,
		loops:     loops,
		metaTable: metaTable.Table(),
		pool:      pool,
		stagers:   stagers,
		stop:      ctx,
		watchers:  watchers,
	}
	ret.mu.instances = &ident.SchemaMap[*logical.Loop]{}

	// Resume from previous state.
	schemas, err := ScanForTargetSchemas(ctx, pool, ret.metaTable)
	if err != nil {
		return nil, err
	}
	for _, schema := range schemas {
		if _, _, err := ret.get(ctx, schema); err != nil {
			return nil, errors.Wrapf(err, "could not bootstrap resolver for schema %s", schema)
		}
	}

	return ret, nil
}
