// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemawatch

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v5/pgxpool"
)

// factory is a memoizing factory for watcher instances.
type factory struct {
	pool *pgxpool.Pool
	mu   struct {
		sync.RWMutex
		cancels []func()
		data    map[ident.Ident]*watcher
	}
}

var _ types.Watchers = (*factory)(nil)

// Get creates or returns a memoized watcher for the given database.
func (f *factory) Get(ctx context.Context, db ident.Ident) (types.Watcher, error) {
	if ret := f.getUnlocked(db); ret != nil {
		return ret, nil
	}
	return f.createUnlocked(ctx, db)
}

// close destroys all watcher instances associated with the factory.
func (f *factory) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, cancel := range f.mu.cancels {
		cancel()
	}
	f.mu.cancels = nil
	f.mu.data = make(map[ident.Ident]*watcher)
}

func (f *factory) createUnlocked(ctx context.Context, db ident.Ident) (*watcher, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.data[db]; ret != nil {
		return ret, nil
	}

	ret, cancel, err := newWatcher(ctx, f.pool, db)
	if err != nil {
		return nil, err
	}

	f.mu.cancels = append(f.mu.cancels, cancel)
	f.mu.data[db] = ret
	return ret, nil
}

func (f *factory) getUnlocked(db ident.Ident) *watcher {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.data[db]

}
