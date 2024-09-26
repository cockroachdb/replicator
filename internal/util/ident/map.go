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

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/cockroachdb/replicator/internal/util/cmap"
	"github.com/pkg/errors"
)

// Map is a case-insensitive mapping of Ident to values.
type Map[V any] struct {
	IdentifierMap[Ident, V]
}

// MapOf accepts an even number of arguments and constructs a map of
// identifiers to values. The identifiers may be given as either an
// Ident or as a string. This function panics if there is invalid input,
// so its use should be limited to testing.
func MapOf[V any](args ...any) *Map[V] {
	if len(args)%2 != 0 {
		panic("expecting an even number of arguments")
	}
	ret := &Map[V]{}
	for i := 0; i < len(args); i += 2 {
		var key Ident
		switch t := args[i].(type) {
		case Ident:
			key = t
		case string:
			key = New(t)
		default:
			panic(fmt.Sprintf("unsupported type %T at index %d", t, i))
		}
		// A nil in the input should result in a zero value.
		var v V
		if args[i+1] != nil {
			v = args[i+1].(V)
		}
		ret.Put(key, v)
	}
	return ret
}

// Equal returns true if the other map is equivalent under identifier
// case-equivalency. This method exists to reduce the amount of generics
// syntax needed to call [cmap.Equal].
func (m *Map[V]) Equal(o *Map[V], comparator func(V, V) bool) bool {
	return cmap.Equal[Ident, V](m, o, comparator)
}

// SchemaMap is a case-insensitive mapping of Schema to values.
type SchemaMap[V any] struct {
	IdentifierMap[Schema, V]
}

// TableMap is a case-insensitive mapping of Table to values.
type TableMap[V any] struct {
	IdentifierMap[Table, V]
}

// IdentifierMap implements a case-preserving, but case-insensitive
// mapping of Identifier instances to values. Prefer using the
// specialized types instead.
//
// Usage notes:
//   - The zero value of an IdentifierMap is safe to use.
//   - An IdentifierMap can be serialized to and from a JSON
//     representation.
//   - An IdentifierMap is not internally synchronized.
type IdentifierMap[I Identifier, V any] struct {
	data cmap.Map[I, V]
	_    noCopy
}

var (
	_ cmap.Map[Identifier, any] = (*IdentifierMap[Identifier, any])(nil)
	_ json.Marshaler            = (*IdentifierMap[Identifier, any])(nil)
	_ json.Unmarshaler          = (*IdentifierMap[Identifier, any])(nil)
)

func (m *IdentifierMap[I, V]) newMap() cmap.Map[I, V] {
	return cmap.New[I, any, V](func(ident I) any {
		switch t := Identifier(ident).(type) {
		case *atom:
			return t.lowered
		case *array:
			return t.lowered
		case *qualified:
			return t.lowered
		case Ident:
			if t.atom == nil {
				return (*atom)(nil)
			}
			return t.atom.lowered
		case Schema:
			if t.array == nil {
				return (*array)(nil)
			}
			return t.array.lowered
		case Table:
			if t.qualified == nil {
				return (*qualified)(nil)
			}
			return t.qualified.lowered
		default:
			panic(fmt.Sprintf("unimplemented: %T", t))
		}
	})
}

// All implements cmap.Map.
func (m *IdentifierMap[I, V]) All() iter.Seq2[I, V] {
	if m.data != nil {
		return m.data.All()
	}
	return func(yield func(I, V) bool) {}
}

// CopyInto implements cmap.Map.
func (m *IdentifierMap[I, V]) CopyInto(dest cmap.Map[I, V]) {
	if m.data != nil {
		m.data.CopyInto(dest)
	}
}

// Delete implements cmap.Map.
func (m *IdentifierMap[I, V]) Delete(key I) {
	if m.data != nil {
		m.data.Delete(key)
	}
}

// Get implements cmap.Map.
func (m *IdentifierMap[I, V]) Get(key I) (_ V, ok bool) {
	if m.data != nil {
		return m.data.Get(key)
	}
	return *new(V), false
}

// GetZero implements cmap.Map.
func (m *IdentifierMap[I, V]) GetZero(key I) V {
	if m.data != nil {
		return m.data.GetZero(key)
	}
	return *new(V)
}

func (m *IdentifierMap[I, V]) Keys() iter.Seq[I] {
	if m.data != nil {
		return m.data.Keys()
	}
	return func(yield func(I) bool) {}
}

// Len implements cmap.Map.
func (m *IdentifierMap[I, V]) Len() int {
	if m.data != nil {
		return m.data.Len()
	}
	return 0
}

// Match implements cmap.Map.
func (m *IdentifierMap[I, V]) Match(key I) (_ I, _ V, ok bool) {
	if m.data != nil {
		return m.data.Match(key)
	}
	return *new(I), *new(V), false
}

// Put implements cmap.Map
func (m *IdentifierMap[I, V]) Put(key I, value V) {
	if m.data == nil {
		m.data = m.newMap()
	}
	m.data.Put(key, value)
}

// MarshalJSON implements json.Marshaler.
func (m *IdentifierMap[I, V]) MarshalJSON() ([]byte, error) {
	temp := make(map[string]V)
	for k, v := range m.All() {
		temp[k.Raw()] = v
	}
	return json.Marshal(temp)
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *IdentifierMap[I, V]) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // Preserve large-magnitude JSON numbers.
	temp := make(map[string]V)
	if err := dec.Decode(&temp); err != nil {
		return errors.WithStack(err)
	}

	next := m.newMap()
	switch t := (any)(*new(I)).(type) {
	case Ident:
		for k, v := range temp {
			id := New(k)
			next.Put(Identifier(id).(I), v)
		}

	case Schema:
		for k, v := range temp {
			sch, err := ParseSchema(k)
			if err != nil {
				return err
			}
			next.Put(Identifier(sch).(I), v)
		}

	case Table:
		for k, v := range temp {
			tbl, err := ParseTable(k)
			if err != nil {
				return err
			}
			next.Put(Identifier(tbl).(I), v)
		}
	default:
		return errors.Errorf("unimplemented: %T", t)
	}
	m.data = next
	return nil
}

// Values returns an iterator over the map values.
func (m *IdentifierMap[I, V]) Values() iter.Seq[V] {
	if m.data != nil {
		return m.data.Values()
	}
	return func(yield func(V) bool) {}
}
