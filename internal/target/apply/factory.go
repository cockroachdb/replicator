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

package apply

import (
	"context"
	"sync"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/target/load"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/applycfg"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// factory vends singleton instance of apply.
type factory struct {
	cache    *types.TargetStatements
	configs  *applycfg.Configs
	dlqs     types.DLQs
	loader   *load.Loader
	poolInfo *types.PoolInfo
	stop     *stopper.Context
	watchers types.Watchers
	mu       struct {
		sync.RWMutex
		instances *ident.TableMap[*apply]
	}
}

var _ diag.Diagnostic = (*factory)(nil)

// Diagnostic implements [diag.Diagnostic].
func (f *factory) Diagnostic(_ context.Context) any {
	ret := &ident.TableMap[any]{}

	f.mu.RLock()
	defer f.mu.RUnlock()

	_ = f.mu.instances.Range(func(tbl ident.Table, impl *apply) error {
		impl.mu.RLock()
		defer impl.mu.RUnlock()
		ret.Put(tbl, impl.mu.templates.columnMapping)
		return nil
	})

	return ret
}

// Get creates or returns a memoized instance of the table's Applier.
func (f *factory) Get(_ context.Context, table ident.Table) (*apply, error) {
	// Try read-locked get.
	if ret := f.getUnlocked(table); ret != nil {
		return ret, nil
	}
	// Fall back to write-locked get-or-create.
	return f.getOrCreateUnlocked(f.poolInfo, table)
}

// getOrCreateUnlocked takes a write-lock.
func (f *factory) getOrCreateUnlocked(poolInfo *types.PoolInfo, table ident.Table) (*apply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances.GetZero(table); ret != nil {
		return ret, nil
	}
	ret, err := f.newApply(f.stop, poolInfo, table)
	if err == nil {
		f.mu.instances.Put(table, ret)
	}
	return ret, err
}

// getUnlocked takes a read-lock.
func (f *factory) getUnlocked(table ident.Table) *apply {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances.GetZero(table)
}
