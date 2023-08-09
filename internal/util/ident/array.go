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

// This is a function of the maximum namespace depth of the products
// that we support (i.e. CockroachDB).
const maxArrayLength = 2

// arrayKey is an array type of fixed length, since we can, at least
// for now, place a finite upper bound on the number of namespace levels
// supported.
type arrayKey [maxArrayLength]*atom

// An array is a canonicalized instance of an array of atoms. They
// should only ever be constructed via [arrays].
type array struct {
	key     arrayKey
	lowered *array
	_       noCopy
}

var arrays = canonicalMap[arrayKey, *array]{
	Lazy: func(owner *canonicalMap[arrayKey, *array], key arrayKey) *array {
		var loweredKey arrayKey
		for idx, atom := range key {
			if atom == nil {
				break
			}
			loweredKey[idx] = atom.lowered
		}

		ret := &array{key: key}
		if loweredKey == key {
			ret.lowered = ret
		} else {
			ret.lowered = owner.Get(loweredKey)
		}
		return ret
	},
}

// Empty returns true if the Schema has no name.
func (a *array) Empty() bool {
	if a == nil {
		return true
	}
	for _, atm := range a.key {
		if !atm.Empty() {
			return false
		}
	}
	return true
}

// Idents implements HasIdents.
func (a *array) Idents(buf []Ident) []Ident {
	if a.Empty() {
		return buf
	}
	if len(buf) == 0 {
		buf = make([]Ident, 0, len(a.key))
	}
	for _, i := range a.key {
		if i == nil {
			break
		}
		buf = append(buf, Ident{atom: i})
	}
	return buf
}

// MarshalJSON returns the Schema as an array.
func (a *array) MarshalJSON() ([]byte, error) {
	return joinJSON(a)
}

// MarshalText returns the raw, dotted form of the Schema.
func (a *array) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// Raw returns the original, raw value.
func (a *array) Raw() string {
	return Join(a, Raw, separator)
}

// Split returns the first atom as an Ident and an array for the remainder.
func (a *array) Split() (Ident, Identifier) {
	if a.Empty() {
		return Ident{}, empty
	}
	var nextKey arrayKey
	copy(nextKey[:], a.key[1:])
	return Ident{atom: a.key[0]}, arrays.Get(nextKey)
}

// String returns the identifier in a manner suitable for constructing a
// query.
func (a *array) String() string {
	return Join(a, Quoted, separator)
}
