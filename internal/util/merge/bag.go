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

package merge

import (
	"bytes"
	"encoding/json"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cmap"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// An Entry associates a column with some reified value.
type Entry struct {
	Column *types.ColData
	Valid  bool // Tri-state to indicate that Value has been set, possibly to nil.
	Value  any  // Prefer checking Valid instead of comping Value to nil.
}

// A BagSpec holds common metadata about property structures.
type BagSpec struct {
	Columns []types.ColData         // Schema data that defines the Bag.
	Rename  *ident.Map[ident.Ident] // (Optional) Mapping applied to all keys passed to API.
}

// A Bag is a loosely-typed map of named properties that are associated
// with schema data. Properties defined within in a bag are either
// mapped (associated with a column mapping) or unmapped (loose).
//
// Bag also supports transparent renaming of properties.
//
// A Bag is not internally synchronized.
type Bag struct {
	*BagSpec // Common metadata about property structure.

	// Mapped contains an Entry for each known column. This makes it
	// simple to identify whether any given API call to a Bag should act
	// on a mapped or an unmapped property.  If it is a mapped property,
	// there will already be an entry in this map.
	//
	// The Entry type has a Valid flag which allows callers to
	// distinguish unset properties from ones that are explicitly set to
	// nil. The methods on Bag will automatically filter out mapped,
	// invalid properties.
	Mapped   *ident.Map[*Entry]
	Meta     map[string]any  // External metadata, derived from from decoded Mutation.
	Unmapped *ident.Map[any] // Properties not present in Columns.
}

var (
	_ cmap.Map[ident.Ident, any] = (*Bag)(nil)
	_ json.Unmarshaler           = (*Bag)(nil)
	_ json.Marshaler             = (*Bag)(nil)
)

// NewBag creates an empty Bag for the given columns.
func NewBag(spec *BagSpec) *Bag {
	ret := &Bag{
		BagSpec:  spec,
		Mapped:   &ident.Map[*Entry]{},
		Unmapped: &ident.Map[any]{},
	}

	// Seed the mapped columns with invalid entries.
	for idx := range ret.Columns {
		name := ret.Columns[idx].Name
		ret.Mapped.Put(name, &Entry{
			Column: &ret.Columns[idx],
		})
	}

	return ret
}

// NewBagOf is a test helper to construct a Bag from a varargs
// consisting of keys and values.
func NewBagOf(colData []types.ColData, rename *ident.Map[ident.Ident], args ...any) *Bag {
	ret := NewBag(&BagSpec{colData, rename})
	ident.MapOf[any](args...).CopyInto(ret)
	return ret
}

// NewBagFrom returns an empty Bag whose schema information is copied
// from the given source.
func NewBagFrom(b *Bag) *Bag {
	return NewBag(b.BagSpec)
}

// CopyInto implements cmap.Map.
func (b *Bag) CopyInto(dest cmap.Map[ident.Ident, any]) {
	// Ignoring error since callback returns nil.
	_ = b.Mapped.Range(func(k ident.Ident, v *Entry) error {
		if v.Valid {
			dest.Put(k, v.Value)
		}
		return nil
	})
	b.Unmapped.CopyInto(dest)
}

// Delete implements cmap.Map.
func (b *Bag) Delete(key ident.Ident) {
	key = b.renamed(key)
	if entry, ok := b.Mapped.Get(key); ok {
		entry.Valid = false
		entry.Value = nil
		return
	}
	b.Unmapped.Delete(key)
}

// Entries implements cmap.Map.
func (b *Bag) Entries() []cmap.Entry[ident.Ident, any] {
	ret := make([]cmap.Entry[ident.Ident, any], 0, b.Len())
	// Ignoring error since callback returns nil.
	_ = b.Mapped.Range(func(k ident.Ident, v *Entry) error {
		if v.Valid {
			ret = append(ret, cmap.Entry[ident.Ident, any]{Key: k, Value: v.Value})
		}
		return nil
	})
	_ = b.Unmapped.Range(func(k ident.Ident, v any) error {
		ret = append(ret, cmap.Entry[ident.Ident, any]{Key: k, Value: v})
		return nil
	})
	return ret
}

// Entry returns the mapped Entry, or returns false.
func (b *Bag) Entry(key ident.Ident) (_ *Entry, ok bool) {
	key = b.renamed(key)
	return b.Mapped.Get(key)
}

// Get implements cmap.Map.
func (b *Bag) Get(key ident.Ident) (_ any, ok bool) {
	key = b.renamed(key)
	if entry, ok := b.Mapped.Get(key); ok {
		if entry.Valid {
			return entry.Value, true
		}
		return nil, false
	}
	return b.Unmapped.Get(key)
}

// GetZero implements cmap.Map.
func (b *Bag) GetZero(key ident.Ident) any {
	ret, _ := b.Get(key)
	return ret
}

// Len implements cmap.Map and reports the number of properties with
// valid values.
func (b *Bag) Len() int {
	ct := 0
	// Ignoring error since callback returns nil.
	_ = b.Mapped.Range(func(_ ident.Ident, entry *Entry) error {
		if entry.Valid {
			ct++
		}
		return nil
	})
	return ct + b.Unmapped.Len()
}

// Match implements cmap.Map
func (b *Bag) Match(key ident.Ident) (_ ident.Ident, _ any, ok bool) {
	key = b.renamed(key)
	if k, v, ok := b.Mapped.Match(key); ok {
		if v.Valid {
			return k, v.Value, true
		}
		return ident.Ident{}, nil, false
	}
	return b.Unmapped.Match(key)
}

// MarshalJSON implements json.Marshaler and serializes the contents of
// the Bag, but not its schema, as a JSON object.
func (b *Bag) MarshalJSON() ([]byte, error) {
	// Revisit when streaming JSON API is available.
	// https://github.com/golang/go/discussions/63397
	var temp ident.Map[any]
	b.CopyInto(&temp)
	return json.Marshal(&temp)
}

// Put implements cmap.Map.
func (b *Bag) Put(key ident.Ident, value any) {
	key = b.renamed(key)
	if entry, ok := b.Mapped.Get(key); ok {
		entry.Valid = true
		entry.Value = value
		return
	}
	b.Unmapped.Put(key, value)
}

// Range implements cmap.Map. It will emit the values of all valid
// properties, which could be nil.
func (b *Bag) Range(fn func(k ident.Ident, v any) error) error {
	if err := b.Mapped.Range(func(k ident.Ident, v *Entry) error {
		if v.Valid {
			return fn(k, v.Value)
		}
		return nil
	}); err != nil {
		return err
	}
	return b.Unmapped.Range(fn)
}

// UnmarshalJSON appends a JSON object to the contents of the Bag.
func (b *Bag) UnmarshalJSON(data []byte) error {
	var temp ident.Map[any]
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&temp); err != nil {
		return errors.WithStack(err)
	}
	temp.CopyInto(b)
	return nil
}

// renamed returns the key if it is not present in the Rename map.
// Otherwise, it returns the renamed key.
func (b *Bag) renamed(key ident.Ident) ident.Ident {
	if b.Rename == nil {
		return key
	}
	if found, ok := b.Rename.Get(key); ok {
		return found
	}
	return key
}
