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

package schemawatch

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// factory is a memoizing factory for watcher instances.
type factory struct {
	pool *types.TargetPool
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
