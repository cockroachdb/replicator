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
	"github.com/jackc/pgx/v4/pgxpool"
)

// Watchers is a memoizing factory for Watcher instances.
type Watchers struct {
	pool *pgxpool.Pool
	mu   struct {
		sync.RWMutex
		cancels []func()
		data    map[ident.Ident]*Watcher
	}
}

var _ types.Watchers = (*Watchers)(nil)

// NewWatchers creates a Watchers factory.
func NewWatchers(pool *pgxpool.Pool) (_ *Watchers, cancel func()) {
	w := &Watchers{pool: pool}
	w.mu.data = make(map[ident.Ident]*Watcher)
	return w, w.close
}

// Get creates or returns a memoized Watcher for the given database.
func (w *Watchers) Get(ctx context.Context, db ident.Ident) (types.Watcher, error) {
	if ret := w.getUnlocked(db); ret != nil {
		return ret, nil
	}
	return w.createUnlocked(ctx, db)
}

// close destroys all Watcher instances associated with the factory.
func (w *Watchers) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cancel := range w.mu.cancels {
		cancel()
	}
	w.mu.cancels = nil
	w.mu.data = make(map[ident.Ident]*Watcher)
}

func (w *Watchers) createUnlocked(ctx context.Context, db ident.Ident) (*Watcher, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if ret := w.mu.data[db]; ret != nil {
		return ret, nil
	}

	ret, cancel, err := newWatcher(ctx, w.pool, db)
	if err != nil {
		return nil, err
	}

	w.mu.cancels = append(w.mu.cancels, cancel)
	w.mu.data[db] = ret
	return ret, nil
}

func (w *Watchers) getUnlocked(db ident.Ident) *Watcher {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.mu.data[db]

}
