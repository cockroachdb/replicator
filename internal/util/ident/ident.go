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

// Package ident contains types for safely representing SQL identifiers.
package ident

import (
	"encoding/json"
	"sort"
)

// Public is a commonly-used identifier.
var Public = New("public")

// An Ident is a quoted SQL identifier, generally a table, column, or
// database.
type Ident struct {
	_ noCompare
	*atom
}

// New returns a quoted SQL identifier. Prefer using ParseIdent when
// operating on user-provided input that may already be quoted.
func New(raw string) Ident {
	return Ident{atom: atoms.Get(raw)}
}

// UnmarshalJSON converts a raw json string into an Ident.
func (n *Ident) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*n = New(raw)
	return nil
}

// UnmarshalText converts a raw string into an Ident.
func (n *Ident) UnmarshalText(data []byte) error {
	*n = New(string(data))
	return nil
}

// Idents is a slice of Ident.
type Idents []Ident

var _ sort.Interface = Idents(nil)

// Equal returns true if the two slices contain equivalent identifiers.
func (n Idents) Equal(o Idents) bool {
	if len(n) != len(o) {
		return false
	}
	for i := range n {
		if !Equal(n[i], o[i]) {
			return false
		}
	}
	return true
}

// Len implements [sort.Interface].
func (n Idents) Len() int { return len(n) }

// Less implements [sort.Interface].
func (n Idents) Less(i, j int) bool { return Compare(n[i], n[j]) < 0 }

// Swap implements [sort.Interface].
func (n Idents) Swap(i, j int) { n[i], n[j] = n[j], n[i] }
