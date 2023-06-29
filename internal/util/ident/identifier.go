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
	"encoding"
	"encoding/json"
	"strings"
)

// Identifier represents some, possibly-compound, name in an external
// system.
type Identifier interface {
	encoding.TextMarshaler
	json.Marshaler

	// Empty returns true if the Identifier is blank.
	Empty() bool

	// Idents appends the components of the Identifier to the buffer and
	// returns it. It is valid to pass a nil value for the buffer.
	Idents(buf []Ident) []Ident

	// Raw returns an unquoted representation of the Identifier.
	Raw() string

	// String returns a quoted, concatenation-safe representation of the
	// Identifier that is suitable for use when building queries.
	String() string
}

var (
	_ Identifier = Ident{}
	_ Identifier = Schema{}
	_ Identifier = Table{}
	_ Identifier = (*array)(nil)
	_ Identifier = (*atom)(nil)
	_ Identifier = (*qualified)(nil)
)

// Join returns the identifiers as a string with the given separator.
func Join(x Identifier, raw bool, separator rune) string {
	parts := make([]Ident, 0, maxArrayLength+1)
	parts = x.Idents(parts)

	var sb strings.Builder
	for idx, part := range parts {
		if idx > 0 {
			sb.WriteRune(separator)
		}
		if raw {
			sb.WriteString(part.Raw())
		} else {
			sb.WriteString(part.String())
		}
	}
	return sb.String()
}

// joinJSON returns the identifiers as a JSON array.
func joinJSON(x Identifier) ([]byte, error) {
	parts := make([]Ident, 0, +1)
	parts = x.Idents(parts)
	return json.Marshal(parts)
}
