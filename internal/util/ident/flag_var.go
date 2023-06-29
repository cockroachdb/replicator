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

// Value allows Idents to be used with the spf13 flags package.
type Value Ident

// NewValue wraps the given Ident so that it can be used with the spf13
// flags package.
func NewValue(value string, id *Ident) *Value {
	*id = New(value)
	return (*Value)(id)
}

// Set implements Value.
func (v *Value) Set(s string) error { *(*Ident)(v) = New(s); return nil }

// String returns the raw value of the underlying Ident.
func (v *Value) String() string { return (*Ident)(v).Raw() }

// Type implements Value.
func (v *Value) Type() string { return "ident" }

// SchemaFlag allows Schema fields to be used with the spf13 flags package.
type SchemaFlag Schema

// NewSchemaFlag wraps the given Schema so that it can be used with the
// spf13 flags package.
func NewSchemaFlag(id *Schema) *SchemaFlag {
	return (*SchemaFlag)(id)
}

// Set implements Value.
func (v *SchemaFlag) Set(s string) error {
	parsed, err := ParseSchema(s)
	if err == nil {
		*(*Schema)(v) = parsed
	}
	return err
}

// String returns the raw value of the underlying Ident.
func (v *SchemaFlag) String() string { return v.Raw() }

// Type implements Value.
func (v *SchemaFlag) Type() string { return "atom" }
