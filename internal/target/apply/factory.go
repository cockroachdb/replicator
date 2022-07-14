// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// factory vends singleton instance of apply.
type factory struct {
	configs  *Configs
	watchers types.Watchers
	mu       struct {
		sync.RWMutex
		cleanup   []func()
		instances map[ident.Table]*apply
	}
}

var _ types.Appliers = (*factory)(nil)

// Get creates or returns a memoized instance of the table's Applier.
func (f *factory) Get(_ context.Context, table ident.Table) (types.Applier, error) {
	// Try read-locked get.
	if ret := f.getUnlocked(table); ret != nil {
		return ret, nil
	}
	// Fall back to write-locked get-or-create.
	return f.getOrCreateUnlocked(table)
}

// getOrCreateUnlocked takes a write-lock.
func (f *factory) getOrCreateUnlocked(table ident.Table) (*apply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances[table]; ret != nil {
		return ret, nil
	}
	ret, cancel, err := newApply(table, f.configs, f.watchers)
	if err == nil {
		f.mu.cleanup = append(f.mu.cleanup, cancel)
		f.mu.instances[table] = ret
	}
	return ret, err
}

// getUnlocked takes a read-lock.
func (f *factory) getUnlocked(table ident.Table) *apply {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances[table]
}
