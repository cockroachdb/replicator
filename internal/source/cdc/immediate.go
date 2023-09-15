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

package cdc

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// Immediate memoizes instances of [logical.Batcher].
type Immediate struct {
	loops *logical.Factory

	mu struct {
		sync.RWMutex
		cleanup []func()
		targets ident.SchemaMap[logical.Batcher]
	}
}

// cleanup is called by ProvideImmediate.
func (f *Immediate) cleanup() {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, cleanup := range f.mu.cleanup {
		cleanup()
	}
	f.mu.cleanup = nil
	f.mu.targets = ident.SchemaMap[logical.Batcher]{}
}

// Get returns a facade to apply mutations to the target schema.
func (f *Immediate) Get(ctx context.Context, target ident.Schema) (logical.Batcher, error) {
	f.mu.RLock()
	found, ok := f.mu.targets.Get(target)
	f.mu.RUnlock()
	if ok {
		return found, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check.
	if found, ok := f.mu.targets.Get(target); ok {
		return found, nil
	}

	ret, cancel, err := f.loops.Immediate(ctx, target)
	if err != nil {
		return nil, err
	}
	f.mu.cleanup = append(f.mu.cleanup, cancel)
	f.mu.targets.Put(target, ret)
	return ret, nil
}
