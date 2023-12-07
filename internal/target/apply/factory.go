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

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
)

// factory vends singleton instance of apply.
type factory struct {
	cache    *types.TargetStatements
	configs  *applycfg.Configs
	dlqs     types.DLQs
	product  types.Product
	stop     *stopper.Context
	watchers types.Watchers
	mu       struct {
		sync.RWMutex
		instances *ident.TableMap[types.Applier]
	}
}

var (
	_ types.Appliers  = (*factory)(nil)
	_ diag.Diagnostic = (*factory)(nil)
)

// Diagnostic implements [diag.Diagnostic].
func (f *factory) Diagnostic(ctx context.Context) any {
	ret := &ident.TableMap[any]{}

	f.mu.RLock()
	defer f.mu.RUnlock()

	_ = f.mu.instances.Range(func(tbl ident.Table, applier types.Applier) error {
		if impl, ok := applier.(*apply); ok {
			impl.mu.RLock()
			defer impl.mu.RUnlock()
			ret.Put(tbl, impl.mu.templates.columnMapping)
		} else if d, ok := applier.(diag.Diagnostic); ok {
			ret.Put(tbl, d.Diagnostic(ctx))
		}
		return nil
	})

	return ret
}

// Get creates or returns a memoized instance of the table's Applier.
func (f *factory) Get(_ context.Context, table ident.Table) (types.Applier, error) {
	// Try read-locked get.
	if ret := f.getUnlocked(table); ret != nil {
		return ret, nil
	}
	// Fall back to write-locked get-or-create.
	return f.getOrCreateUnlocked(f.product, table)
}

// getOrCreateUnlocked takes a write-lock.
func (f *factory) getOrCreateUnlocked(
	product types.Product, table ident.Table,
) (types.Applier, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances.GetZero(table); ret != nil {
		return ret, nil
	}
	// Allow implementations to be injected.
	cfg, _ := f.configs.Get(table).Get()
	if del := cfg.Delegate; del != nil {
		f.mu.instances.Put(table, del)
		return del, nil
	}
	ret, err := f.newApply(f.stop, product, table)
	if err == nil {
		f.mu.instances.Put(table, ret)
	}
	return ret, err
}

// getUnlocked takes a read-lock.
func (f *factory) getUnlocked(table ident.Table) types.Applier {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances.GetZero(table)
}
