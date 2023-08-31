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

	"github.com/pkg/errors"
)

// A UDT is the name of a user-defined type, such as an enum.
type UDT struct {
	_ noCompare
	*qualified

	// TODO(bob): Remove this if we add a proper typesystem, since we'd
	// have an array type, containing an element that represents the
	// UDT.
	array bool
}

// NewUDT constructs an identifier for a user-defined type.
func NewUDT(enclosing Schema, name Ident) UDT {
	return UDT{qualified: qualifieds.Get(qualifiedKey{enclosing.array, name.atom})}
}

// NewUDTArray constructs an identifier for a UDT array.
func NewUDTArray(enclosing Schema, name Ident) UDT {
	return UDT{
		array:     true,
		qualified: qualifieds.Get(qualifiedKey{enclosing.array, name.atom}),
	}
}

// IsArray returns true if the UDT is an array type.
func (t UDT) IsArray() bool {
	return t.array
}

// Name returns the UDT's leaf name identifier.
func (t UDT) Name() Ident {
	if t.qualified == nil {
		return Ident{}
	}
	return Ident{atom: t.qualified.terminal}
}

// MarshalJSON implements json.Marshaler.
func (t UDT) MarshalJSON() ([]byte, error) {
	return json.Marshal(&encodedUDT{
		Array: t.array,
		Name:  Table{qualified: t.qualified},
	})
}

// MarshalText implements encoding.TextMarshaler.
func (t *UDT) MarshalText() ([]byte, error) {
	return []byte(t.Raw()), nil
}

// Raw implements Identifier.
func (t UDT) Raw() string {
	ret := t.qualified.Raw()
	if t.IsArray() {
		ret += "[]"
	}
	return ret
}

// Schema returns the schema from the UDT.
func (t UDT) Schema() Schema {
	if t.qualified == nil {
		return Schema{}
	}
	return Schema{array: t.qualified.namespace}
}

// String returns the quoted form of the UDT, possibly with an array
// suffix.
func (t UDT) String() string {
	ret := t.qualified.String()
	if t.IsArray() {
		ret += "[]"
	}
	return ret
}

// UnmarshalJSON parses a JSON array.
func (t *UDT) UnmarshalJSON(data []byte) error {
	var enc encodedUDT
	if err := json.Unmarshal(data, &enc); err != nil {
		return errors.WithStack(err)
	}
	t.array = enc.Array
	t.qualified = enc.Name.qualified
	return nil
}

type encodedUDT struct {
	Array bool  `json:"array,omitempty"`
	Name  Table `json:"name,omitempty"` // Borrow encoding implementation.
}
