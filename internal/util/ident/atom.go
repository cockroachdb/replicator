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

// atom are canonicalized instance of a quoted identifier. They should
// only ever be constructed via [atoms].
type atom struct {
	q, r string
	_    noCopy
}

// Keys are raw values.
var atoms = canonicalMap[string, *atom]{
	Lazy: func(key string) *atom {
		return &atom{
			q: `"` + strings.ReplaceAll(key, `"`, `""`) + `"`,
			r: key,
		}
	},
}

// Empty implements Identifier and returns true if the identifier is empty.
func (i *atom) Empty() bool {
	return i == nil || i.r == ""
}

// Idents implement Identifier.
func (i *atom) Idents(buf []Ident) []Ident {
	if i == nil {
		return buf
	}
	return append(buf, Ident{i})
}

// MarshalJSON implements Identifier, returning a JSON string.
func (i *atom) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.Raw())
}

// MarshalText returns the Identifier's raw form, allowing the value to
// be used as a JSON map-key.
func (i *atom) MarshalText() ([]byte, error) {
	if i == nil {
		return nil, nil
	}
	return []byte(i.Raw()), nil
}

// Raw implements Identifier and returns the original, raw value.
func (i *atom) Raw() string {
	if i == nil {
		return ""
	}
	return i.r
}

// String returns the atom in a manner suitable for constructing a query.
func (i *atom) String() string {
	if i == nil {
		return `""`
	}
	return i.q
}

// noCopy idiom for go vet.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
