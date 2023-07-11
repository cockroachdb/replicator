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
	"encoding/json"
	"strings"
)

// An atom is a canonicalized instance of a quoted identifier. They
// should only ever be constructed via [atoms].
type atom struct {
	quoted, raw string
	_           noCopy
}

// Keys are raw values.
var atoms = canonicalMap[string, *atom]{
	Lazy: func(key string) *atom {
		return &atom{
			quoted: `"` + strings.ReplaceAll(key, `"`, `""`) + `"`,
			raw:    key,
		}
	},
}

// Empty implements Identifier and returns true if the identifier is empty.
func (a *atom) Empty() bool {
	return a == nil || a.raw == ""
}

// Idents implement Identifier.
func (a *atom) Idents(buf []Ident) []Ident {
	if a == nil {
		return buf
	}
	return append(buf, Ident{a})
}

// MarshalJSON implements Identifier, returning a JSON string.
func (a *atom) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Raw())
}

// MarshalText returns the Identifier's raw form, allowing the value to
// be used as a JSON map-key.
func (a *atom) MarshalText() ([]byte, error) {
	if a == nil {
		return nil, nil
	}
	return []byte(a.Raw()), nil
}

// Raw implements Identifier and returns the original, raw value.
func (a *atom) Raw() string {
	if a == nil {
		return ""
	}
	return a.raw
}

// Split implements Identifier, returning the atom and an empty ident.
func (a *atom) Split() (Ident, Identifier) {
	return Ident{a}, empty
}

// String returns the atom in a manner suitable for constructing a query.
func (a *atom) String() string {
	if a == nil {
		return `""`
	}
	return a.quoted
}
