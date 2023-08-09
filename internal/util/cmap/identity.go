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

package cmap

import "encoding/json"

// NewIdentity returns a Map for comparable keys. A basic map type is
// recommended unless compatibility with the Map interface is necessary.
func NewIdentity[K comparable, V any]() Map[K, V] {
	return make(identity[K, V])
}

type identity[K comparable, V any] map[K]V

func (m identity[K, V]) CopyInto(dest Map[K, V]) {
	for k, v := range m {
		dest.Put(k, v)
	}
}

func (m identity[K, V]) Delete(key K) {
	delete(m, key)
}

func (m identity[K, V]) Entries() []Entry[K, V] {
	ret := make([]Entry[K, V], 0, len(m))
	for k, v := range m {
		ret = append(ret, Entry[K, V]{k, v})
	}
	return ret
}

func (m identity[K, V]) Get(key K) (_ V, ok bool) {
	ret, ok := m[key]
	return ret, ok
}

func (m identity[K, V]) GetZero(key K) V {
	return m[key]
}

func (m identity[K, V]) Len() int {
	return len(m)
}

func (m identity[K, V]) Match(key K) (_ K, _ V, ok bool) {
	ret, ok := m[key]
	if ok {
		return key, ret, true
	}
	return *new(K), *new(V), false
}

func (m identity[K, V]) Put(key K, value V) {
	m[key] = value
}

func (m identity[K, V]) Range(fn func(K, V) error) error {
	for k, v := range m {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (m identity[K, V]) MarshalJSON() ([]byte, error) {
	return json.Marshal(m)
}

func (m identity[K, V]) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, &m)
}
