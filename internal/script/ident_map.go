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
	"github.com/dop251/goja"
)

// identMapWrapper adapts [ident.Map] to [goja.DynamicObject].
type identMapWrapper struct {
	data *ident.Map[any] // Generally expected to be a type returned from the json package.
	rt   *goja.Runtime
}

var _ goja.DynamicObject = (*identMapWrapper)(nil)

func (m *identMapWrapper) Delete(key string) bool {
	m.data.Delete(ident.New(key))
	return true
}

func (m *identMapWrapper) Get(key string) goja.Value {
	v, ok := m.data.Get(ident.New(key))
	if !ok {
		return goja.Undefined()
	}
	return m.rt.ToValue(v)
}

func (m *identMapWrapper) Has(key string) bool {
	_, ok := m.data.Get(ident.New(key))
	return ok
}

func (m *identMapWrapper) Keys() []string {
	ret := make([]string, 0, m.data.Len())
	// Ignoring error since callback returns nil.
	_ = m.data.Range(func(k ident.Ident, v any) error {
		ret = append(ret, k.Raw())
		return nil
	})
	return ret
}

func (m *identMapWrapper) Set(key string, val goja.Value) bool {
	m.data.Put(ident.New(key), val.Export())
	return true
}
