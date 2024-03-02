// Copyright 2024 The Cockroach Authors
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

package crep

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The maximum safe numeric value in JavaScript.
const maxInt = 1 << 53

// The more complicated values will also test Unmarshal.
func TestCanonical(t *testing.T) {
	now := time.UnixMilli(1708731562135).UTC()

	tcs := []struct {
		input      any
		exportType string
		err        string
		expected   Value
		test       func(a *assert.Assertions, value any)
	}{
		{
			input:    nil,
			expected: nil,
		},
		{
			input:      false,
			exportType: "bool",
			expected:   false,
		},
		{
			input:      true,
			exportType: "bool",
			expected:   true,
		},
		{
			input:      "foo",
			exportType: "string",
			expected:   "foo",
		},
		{
			input:      int(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      int8(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      int16(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      int32(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      int64(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      uint(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      uint8(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      uint16(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      uint32(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      uint64(42),
			exportType: "string",
			expected:   "42",
		},
		{
			input:      maxInt + 1,
			exportType: "string",
			expected:   fmt.Sprintf("%d", maxInt+1),
		},
		{
			input:      uint(maxInt + 1),
			exportType: "string",
			expected:   fmt.Sprintf("%d", maxInt+1),
		},
		{
			input:      json.Number("12345"),
			exportType: "string",
			expected:   "12345",
		},
		{
			input:      now,
			exportType: "string",
			expected:   "2024-02-23T23:39:22.135Z",
			test: func(a *assert.Assertions, _ any) {
				a.Equal("2024-02-23 23:39:22.135 +0000 UTC", now.String())
			},
		},
		{
			input:      []any{now, now},
			exportType: "[]crep.Value",
			expected:   []Value{"2024-02-23T23:39:22.135Z", "2024-02-23T23:39:22.135Z"},
		},
		{
			input:      float32(3.141592),
			exportType: "string",
			expected:   "3.141592",
		},
		{
			input:      float64(3.141592),
			exportType: "string",
			expected:   "3.141592",
		},
		{
			input:      json.RawMessage("    3.141592"),
			exportType: "string",
			expected:   "3.141592",
		},
		{
			input:      json.RawMessage("    -3.141592"),
			exportType: "string",
			expected:   "-3.141592",
		},
		{
			input:      json.RawMessage(`  "    3.141592"`),
			exportType: "string",
			expected:   "    3.141592",
		},
		{
			input:    json.RawMessage("null"),
			expected: nil,
		},
		{
			input:      json.RawMessage("true"),
			exportType: "bool",
			expected:   true,
		},
		{
			input:      json.RawMessage("false"),
			exportType: "bool",
			expected:   false,
		},
		{
			input: json.RawMessage(`{
"BigFloat": 9007199254740995.98765,
"BigInt": 9007199254740995,
"Embedded": {
  "Baz": [ 1, 2, 3],
  "EmptyArr": [],
  "EmptyObj": {},
  "Foo": "Bar",
  "Null": null
}
}`),
			exportType: "map[string]crep.Value",
			expected: map[string]Value{
				"BigFloat": "9007199254740995.98765",
				"BigInt":   "9007199254740995",
				"Embedded": map[string]Value{
					"Baz":      []Value{"1", "2", "3"},
					"EmptyArr": []Value(nil),
					"EmptyObj": map[string]Value{},
					"Foo":      "Bar",
					"Null":     nil,
				},
			},
		},
		{
			input:      json.RawMessage(`[9007199254740995]`),
			exportType: "[]crep.Value",
			expected:   []Value{"9007199254740995"},
		},
		{
			input: json.RawMessage(`     `),
			err:   "unexpected EOF",
		},
		{
			input: json.RawMessage(`-123{456`),
			err:   "invalid JSON input",
		},
		{
			// Test missing end delimiter.
			input: json.RawMessage(`[1,2,`),
			err:   "unexpected EOF",
		},
		{
			// Test mismatched delimiter.
			input: json.RawMessage(`[1,2, { "foo": "bar" ] ]`),
			err:   "offset 21: invalid character ']' after object key:value pair",
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			value, err := Canonical(tc.input)
			if tc.err != "" {
				a.ErrorContains(err, tc.err)
			} else if a.NoError(err) {
				if tc.exportType == "" {
					a.Nil(value)
				} else {
					a.Equal(tc.exportType, reflect.TypeOf(value).String())
				}
				a.Equal(tc.expected, value)
				if tc.test != nil {
					tc.test(a, value)
				}
			}
		})
	}
}
