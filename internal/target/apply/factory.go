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
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// cacheKey is used to memoize requests to factory.Get.
type cacheKey string

// factory vends singleton instance of apply.
type factory struct {
	watchers types.Watchers
	mu       struct {
		sync.RWMutex
		cleanup   []func()
		instances map[cacheKey]*apply
	}
}

var _ types.Appliers = (*factory)(nil)

// NewAppliers returns an instance of types.Appliers.
func NewAppliers(watchers types.Watchers) (_ types.Appliers, cancel func()) {
	f := &factory{watchers: watchers}
	f.mu.instances = make(map[cacheKey]*apply)
	return f, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, fn := range f.mu.cleanup {
			fn()
		}
		f.mu.cleanup = nil
		f.mu.instances = nil
	}
}

// Get creates or returns a memoized instance of the table's Applier.
func (f *factory) Get(
	ctx context.Context, table ident.Table, casColumns []ident.Ident, deadlines types.Deadlines,
) (types.Applier, error) {
	// All values below are written as quoted strings, so they are
	// self-delimiting.
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "%s", table)
	for i := range casColumns {
		_, _ = fmt.Fprintf(&sb, "%s", casColumns[i])
	}
	for k, v := range deadlines {
		_, _ = fmt.Fprintf(&sb, "%s%d", k, v)
	}
	key := cacheKey(sb.String())

	// Try read-locked get.
	if ret := f.getUnlocked(key); ret != nil {
		return ret, nil
	}
	// Fall back to write-locked get-or-create.
	return f.getOrCreateUnlocked(ctx, key, table, casColumns, deadlines)
}

// getOrCreateUnlocked takes a write-lock.
func (f *factory) getOrCreateUnlocked(
	ctx context.Context,
	key cacheKey,
	table ident.Table,
	casColumns []ident.Ident,
	deadlines types.Deadlines,
) (*apply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances[key]; ret != nil {
		return ret, nil
	}
	watcher, err := f.watchers.Get(ctx, table.Database())
	if err != nil {
		return nil, err
	}
	ret, cancel, err := newApply(watcher, table, casColumns, deadlines)
	if err == nil {
		f.mu.cleanup = append(f.mu.cleanup, cancel)
		f.mu.instances[key] = ret
	}
	return ret, err
}

// getUnlocked takes a read-lock.
func (f *factory) getUnlocked(key cacheKey) *apply {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances[key]
}
