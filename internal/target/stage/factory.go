// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stage

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4/pgxpool"
)

type factory struct {
	db        *pgxpool.Pool
	stagingDB ident.Ident

	mu struct {
		sync.RWMutex
		instances map[ident.Table]*stage
	}
}

var _ types.Stagers = (*factory)(nil)

// NewStagers returns an instance of types.Stagers that stores
// temporary data in the given SQL database.
func NewStagers(db *pgxpool.Pool, stagingDB ident.Ident) types.Stagers {
	f := &factory{
		db:        db,
		stagingDB: stagingDB,
	}
	f.mu.instances = make(map[ident.Table]*stage)
	return f
}

// Get returns a memoized instance of a stage for the given table.
func (f *factory) Get(ctx context.Context, target ident.Table) (types.Stager, error) {
	if ret := f.getUnlocked(target); ret != nil {
		return ret, nil
	}
	return f.createUnlocked(ctx, target)
}

func (f *factory) createUnlocked(ctx context.Context, table ident.Table) (*stage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances[table]; ret != nil {
		return ret, nil
	}

	ret, err := newStore(ctx, f.db, f.stagingDB, table)
	if err == nil {
		f.mu.instances[table] = ret
	}
	return ret, err
}

func (f *factory) getUnlocked(table ident.Table) *stage {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances[table]
}
