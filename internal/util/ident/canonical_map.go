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

package ident

import "sync"

type canonicalMap[K comparable, V any] struct {
	// This constructor function may be called multiple times with the
	// same key.
	Lazy func(owner *canonicalMap[K, V], key K) V
	// This is ideally a weak map or we could add an intermediate handle
	// object with a finalizer to manage a ref-count scheme. Given the
	// finite, small number of identifiers that we expect to see in a
	// production system, we should be fine with a simple map.
	data map[K]V
	mu   sync.RWMutex
}

// Get implements a thread-safe get-or-create routine.
func (m *canonicalMap[K, V]) Get(key K) V {
	var found V
	var ok bool

	// Read-lock in common case.
	m.mu.RLock()
	if m.data != nil {
		found, ok = m.data[key]
	}
	m.mu.RUnlock()
	if ok {
		return found
	}

	// Construct instance outside the critical section, since the
	// functions will be reentrant.
	ret := m.Lazy(m, key)

	// Upgrade to write lock.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check with write lock to avoid race.
	if m.data == nil {
		m.data = map[K]V{key: ret}
		return ret
	}
	if found, ok := m.data[key]; ok {
		return found
	}

	m.data[key] = ret
	return ret
}
