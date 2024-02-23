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

package script

import (
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/merge"
	"github.com/dop251/goja"
)

// bagWrapper adapts [merge.Bag] to [goja.DynamicObject].
type bagWrapper struct {
	data *merge.Bag
	rt   *goja.Runtime
}

var _ goja.DynamicObject = (*bagWrapper)(nil)

// Delete implements goja.DynamicObject. It always returns true, since
// we never reject any deletes.
func (m *bagWrapper) Delete(key string) bool {
	m.data.Delete(ident.New(key))
	return true
}

// Get implements goja.DynamicObject.
func (m *bagWrapper) Get(key string) goja.Value {
	v, ok := m.data.Get(ident.New(key))
	if !ok {
		return goja.Undefined()
	}
	ret, err := safeValue(m.rt, v)
	if err != nil {
		// This will be presented as an exception.
		panic(m.rt.NewGoError(err))
	}
	return ret
}

// Has implements goja.DynamicObject.
func (m *bagWrapper) Has(key string) bool {
	_, ok := m.data.Get(ident.New(key))
	return ok
}

// Keys implements goja.DynamicObject.
func (m *bagWrapper) Keys() []string {
	ret := make([]string, 0, m.data.Len())
	// Ignoring error since callback returns nil.
	_ = m.data.Range(func(k ident.Ident, v any) error {
		ret = append(ret, k.Raw())
		return nil
	})
	return ret
}

// Set implements goja.DynamicObject. It always returns true since we
// never reject any updates.
func (m *bagWrapper) Set(key string, val goja.Value) bool {
	m.data.Put(ident.New(key), val.Export())
	return true
}
