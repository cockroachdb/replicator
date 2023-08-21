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

	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// factory vends singleton instance of apply.
type factory struct {
	configs  *applycfg.Configs
	product  types.Product
	watchers types.Watchers
	mu       struct {
		sync.RWMutex
		cleanup   []func()
		instances *ident.TableMap[*apply]
	}
}

var (
	_ types.Appliers  = (*factory)(nil)
	_ diag.Diagnostic = (*factory)(nil)
)

// Diagnostic implements [diag.Diagnostic].
func (f *factory) Diagnostic(_ context.Context) any {
	ret := &ident.TableMap[*columnMapping]{}

	f.mu.RLock()
	defer f.mu.RUnlock()

	_ = f.mu.instances.Range(func(tbl ident.Table, app *apply) error {
		app.mu.RLock()
		defer app.mu.RUnlock()
		ret.Put(tbl, app.mu.templates.columnMapping)
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
func (f *factory) getOrCreateUnlocked(product types.Product, table ident.Table) (*apply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.instances == nil {
		return nil, errors.New("factory has been shut down")
	}

	if ret := f.mu.instances.GetZero(table); ret != nil {
		return ret, nil
	}
	ret, cancel, err := newApply(product, table, f.configs, f.watchers)
	if err == nil {
		f.mu.cleanup = append(f.mu.cleanup, cancel)
		f.mu.instances.Put(table, ret)
	}
	return ret, err
}

// getUnlocked takes a read-lock.
func (f *factory) getUnlocked(table ident.Table) *apply {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// Used after shutdown.
	if f.mu.instances == nil {
		return nil
	}
	return f.mu.instances.GetZero(table)
}
