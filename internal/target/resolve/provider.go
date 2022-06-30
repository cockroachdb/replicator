// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolve

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
	ProvideFactory,
)

// MetaTable is an injectable configuration point.
type MetaTable ident.Table

// Table returns the underlying table identifier.
func (t MetaTable) Table() ident.Table { return ident.Table(t) }

// ProvideFactory is called by Wire to construct the resolver. This
// provider will start a background goroutine that runs the resolution
// loop.
func ProvideFactory(
	ctx context.Context,
	appliers types.Appliers,
	metaTable MetaTable,
	pool *pgxpool.Pool,
	stagers types.Stagers,
	timeKeeper types.TimeKeeper,
	watchers types.Watchers,
) (types.Resolvers, func(), error) {
	if _, err := pool.Exec(ctx, fmt.Sprintf(schema, metaTable.Table())); err != nil {
		return nil, func() {}, errors.WithStack(err)
	}

	f := &factory{
		appliers:   appliers,
		metaTable:  metaTable.Table(),
		pool:       pool,
		stagers:    stagers,
		timekeeper: timeKeeper,
		watchers:   watchers,
	}
	f.mu.instances = make(map[ident.Schema]*resolve)

	// Run the bootstrap in a background context.
	bootstrapCtx, cancelBoot := context.WithCancel(context.Background())
	go f.bootstrapResolvers(bootstrapCtx)

	return f, func() {
		defer cancelBoot()

		f.mu.Lock()
		defer f.mu.Unlock()
		for _, fn := range f.mu.cleanup {
			fn()
		}
		f.mu.cleanup = nil
		f.mu.instances = make(map[ident.Schema]*resolve)
	}, nil
}
