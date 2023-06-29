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

import "encoding/json"

type qualifiedKey struct {
	namespace *array
	ident     *atom
}

// A qualified associates an atomic identifier within some namespace.
type qualified struct {
	qualifiedKey
	_ noCopy
}

var qualifieds = canonicalMap[qualifiedKey, *qualified]{
	Lazy: func(key qualifiedKey) *qualified {
		return &qualified{qualifiedKey: key}
	},
}

// Empty implements Identifier.
func (q *qualified) Empty() bool {
	return q == nil || q.namespace.Empty() && q.ident.Empty()
}

// Idents implements Identifier.
func (q *qualified) Idents(buf []Ident) []Ident {
	if q.Empty() {
		return buf
	}
	if len(buf) == 0 {
		buf = make([]Ident, 0, maxArrayLength+1)
	}
	return append(q.namespace.Idents(buf), Ident{q.ident})
}

// MarshalJSON returns the ident as an array.
func (q *qualified) MarshalJSON() ([]byte, error) {
	buf := make([]Ident, 0, maxArrayLength+1)
	buf = q.Idents(buf)
	return json.Marshal(buf)
}

// MarshalText returns the raw, dotted form of the Table.
func (q *qualified) MarshalText() ([]byte, error) {
	return []byte(q.Raw()), nil
}

// Raw implements Identifier.
func (q *qualified) Raw() string {
	return Join(q, true /* raw */, separator)
}

// String returns the identifier in a manner suitable for constructing a
// query.
func (q *qualified) String() string {
	return Join(q, false /* raw */, separator)
}
