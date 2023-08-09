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

// A UDT is the name of a user-defined type, such as an enum.
type UDT struct {
	_ noCompare
	*qualified
}

// NewUDT constructs an identifier for a user-defined type.
func NewUDT(enclosing Schema, name Ident) UDT {
	return UDT{qualified: qualifieds.Get(qualifiedKey{enclosing.array, name.atom})}
}

// Name returns the UDT's leaf name identifier.
func (t UDT) Name() Ident {
	if t.qualified == nil {
		return Ident{}
	}
	return Ident{atom: t.qualified.terminal}
}

// Schema returns the schema from the UDT.
func (t UDT) Schema() Schema {
	if t.qualified == nil {
		return Schema{}
	}
	return Schema{array: t.qualified.namespace}
}

// UnmarshalJSON parses a JSON array.
func (t *UDT) UnmarshalJSON(data []byte) error {
	// Borrow the implementation from Table.
	var tbl Table
	if err := tbl.UnmarshalJSON(data); err != nil {
		return err
	}
	t.qualified = tbl.qualified
	return nil
}
