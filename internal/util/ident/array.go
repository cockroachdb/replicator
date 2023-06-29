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

// array is a canonicalized
type array struct {
	key arrayKey
	_   noCopy
}

var arrays = canonicalMap[arrayKey, *array]{
	Lazy: func(key arrayKey) *array {
		return &array{key: key}
	},
}

// Empty returns true if the Schema has no name.
func (a *array) Empty() bool {
	return a == nil || a.key[0].Empty() && a.key[1].Empty()
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
		buf = append(buf, Ident{i})
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
	return Join(a, true /* raw */, separator)
}

// String returns the identifier in a manner suitable for constructing a
// query.
func (a *array) String() string {
	return Join(a, false /* raw */, separator)
}
