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
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4/pgxpool"
)

type factory struct {
	appliers   types.Appliers
	leases     types.Leases
	metaTable  ident.Table
	pool       *pgxpool.Pool
	stagers    types.Stagers
	timekeeper types.TimeKeeper
	watchers   types.Watchers

	mu struct {
		sync.RWMutex
		cleanup   []func()
		instances map[ident.Schema]*resolve
	}
}

var _ types.Resolvers = (*factory)(nil)

func New(
	ctx context.Context,
	appliers types.Appliers,
	leases types.Leases,
	metaTable ident.Table,
	pool *pgxpool.Pool,
	stagers types.Stagers,
	timekeeper types.TimeKeeper,
) (_ types.Resolvers, cancel func(), _ error) {
	f := &factory{
		appliers:   appliers,
		leases:     leases,
		metaTable:  metaTable,
		pool:       pool,
		stagers:    stagers,
		timekeeper: timekeeper,
	}
	f.mu.instances = make(map[ident.Schema]*resolve)

	return f, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, fn := range f.mu.cleanup {
			fn()
		}
		f.mu.cleanup = nil
		f.mu.instances = make(map[ident.Schema]*resolve)
	}, nil
}

// Get implements types.Resolvers.
func (f *factory) Get(ctx context.Context, target ident.Schema) (types.Resolver, error) {
	if ret := f.getUnlocked(target); ret != nil {
		return ret, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if found, ok := f.mu.instances[target]; ok {
		return found, nil
	}

	ret, cancel, err := newResolve(ctx, f.appliers, f.leases, f.metaTable, f.pool, f.stagers, target, f.timekeeper, f.watchers)
	if err != nil {
		return nil, err
	}
	f.mu.cleanup = append(f.mu.cleanup, cancel)
	f.mu.instances[target] = ret
	return ret, nil
}

func (f *factory) getUnlocked(target ident.Schema) types.Resolver {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances[target]
}
