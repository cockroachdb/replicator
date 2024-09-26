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

// Package cmap contains an implementation of a canonicalizing map.
package cmap

import "iter"

// An Entry in a Map. Note that the Key type is not necessarily
// comparable.
type Entry[K, V any] struct {
	Key   K
	Value V
}

// A Mapper provider a canonical mapping from an arbitrary type to a
// comparable value.
type Mapper[K, C any] func(K) C

// The Map type uses a canonical representation of a key when storing or
// retrieving values. This allows key types which are not comparable
// to be used.
//
// A Map is not internally synchronized.
type Map[K any, V any] interface {
	// All returns an iterator over all map entries.
	All() iter.Seq2[K, V]

	// Clear remove all entries from the map.
	Clear()

	// CopyInto populates the destination with the contents of the map.
	CopyInto(dest Map[K, V])

	// Delete removes a key from the map.
	Delete(key K)

	// Get returns the value associated with the canonical form of the
	// key.
	Get(key K) (_ V, ok bool)

	// GetZero returns the value associated with the canonical form of
	// the key or returns a zero value.
	GetZero(key K) V

	// Keys returns an iterator over the map keys.
	Keys() iter.Seq[K]

	// Len returns the number of entries in the Map.
	Len() int

	// Match is similar to Get, except that it returns the original key
	// used to insert the entry. This can be used by callers to
	// determine if the requested and original keys are an exact match.
	Match(key K) (_ K, _ V, ok bool)

	// Put adds a new mapping. Any error returned by the Mapper will be
	// returned.
	Put(key K, value V)

	// Values returns an iterator over all map values.
	Values() iter.Seq[V]
}

// New constructs a canonicalizing Map. The K type must implement
// TextKey if the MarshalJSON or UnmarshalJSON methods are to be called.
func New[K any, C comparable, V any](mapper Mapper[K, C]) Map[K, V] {
	return &impl[K, C, V]{
		data:   make(map[C]*Entry[K, V]),
		mapper: mapper,
	}
}

// NewOf is a helper for testing. It requires an even number of varargs
// of alternating key and value types. This function will panic, so it
// is not suitable for production code.
func NewOf[K any, C comparable, V any](mapper Mapper[K, C], args ...any) Map[K, V] {
	if len(args)%2 != 0 {
		panic("expecting an even number of arguments")
	}
	ret := New[K, C, V](mapper)
	for i := 0; i < len(args); i += 2 {
		ret.Put(args[i].(K), args[i+1].(V))
	}
	return ret
}

// impl is not exported, since the C type isn't relevant to callers.
type impl[K any, C comparable, V any] struct {
	data   map[C]*Entry[K, V]
	mapper Mapper[K, C]
}

func (m *impl[K, C, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, entry := range m.data {
			if !yield(entry.Key, entry.Value) {
				return
			}
		}
	}
}

func (m *impl[K, C, V]) Clear() {
	clear(m.data)
}

func (m *impl[K, C, V]) CopyInto(dest Map[K, V]) {
	_ = m.Range(func(k K, v V) error {
		dest.Put(k, v)
		return nil
	})
}

func (m *impl[K, C, V]) Delete(key K) {
	c := m.mapper(key)
	delete(m.data, c)
}

func (m *impl[K, C, V]) Get(key K) (_ V, ok bool) {
	_, v, ok := m.Match(key)
	return v, ok
}

func (m *impl[K, C, V]) GetZero(key K) V {
	v, _ := m.Get(key)
	return v
}

func (m *impl[K, C, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for _, entry := range m.data {
			if !yield(entry.Key) {
				return
			}
		}
	}
}

func (m *impl[K, C, V]) Match(key K) (_ K, _ V, ok bool) {
	c := m.mapper(key)
	entry, ok := m.data[c]
	if !ok {
		return *new(K), *new(V), false
	}
	return entry.Key, entry.Value, true
}

func (m *impl[K, C, V]) Len() int {
	return len(m.data)
}

func (m *impl[K, C, V]) Put(key K, value V) {
	c := m.mapper(key)
	m.data[c] = &Entry[K, V]{
		Key:   key,
		Value: value,
	}
}

func (m *impl[K, C, V]) Range(fn func(K, V) error) error {
	for k, v := range m.All() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (m *impl[K, C, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, entry := range m.data {
			if !yield(entry.Value) {
				return
			}
		}
	}
}
