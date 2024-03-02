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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEqual(t *testing.T) {
	now := time.UnixMilli(1708731562135).UTC()

	tcs := []struct {
		a, b  any
		equal bool
		err   string
	}{
		{
			a:     nil,
			b:     nil,
			equal: true,
		},
		{
			a:     true,
			b:     true,
			equal: true,
		},
		{
			a:     false,
			b:     true,
			equal: false,
		},
		{
			a:     "hello",
			b:     "hello",
			equal: true,
		},
		{
			// Bool and string are separate types.
			a:     true,
			b:     "true",
			equal: false,
		},
		{
			// Numbers canonicalize to strings.
			a:     1,
			b:     "1",
			equal: true,
		},
		{
			a:     1,
			b:     "01",
			equal: false,
		},
		{
			// Time canonicalize to strings.
			a:     "2024-02-23T23:39:22.135Z",
			b:     now,
			equal: true,
		},
		{
			// Key order does not matter.
			a: json.RawMessage(`{
"Foo": 1,
"Bar": 2
}`),
			b: json.RawMessage(`{
"Bar": 2,
"Foo": 1
}`),
			equal: true,
		},
		{
			// Differing value
			a: json.RawMessage(`{
"Foo": 1
}`),
			b: json.RawMessage(`{
"Foo": 2
}`),
			equal: false,
		},
		{
			// Mismatch in keys
			a: json.RawMessage(`{
"Foo": 1
}`),
			b: json.RawMessage(`{
"Bar": 1
}`),
			equal: false,
		},
		{
			// Mismatch in number of keys.
			a: json.RawMessage(`{
"Foo": 1,
"Bar": 2
}`),
			b: json.RawMessage(`{
"Bar": 2,
"Foo": 1,
"Quux": false
}`),
			equal: false,
		},
		{
			// Simple array case.
			a:     json.RawMessage(`[ 1 , 2 , 3 ]`),
			b:     json.RawMessage(`[ 1 , 2 , 3 ]`),
			equal: true,
		},
		{
			// Element mismatch.
			a:     json.RawMessage(`[ 3 , 2 , 1 ]`),
			b:     json.RawMessage(`[ 1 , 2 , 3 ]`),
			equal: false,
		},
		{
			// Length mismatch.
			a:     json.RawMessage(`[ 1 , 2 ]`),
			b:     json.RawMessage(`[ 1 , 2 , 3 ]`),
			equal: false,
		},
		{
			// Bad parse in A.
			a:   json.RawMessage(`[ 1 , 2 `),
			b:   json.RawMessage(`[ 1 , 2 , 3 ]`),
			err: "unexpected EOF",
		},
		{
			// Bad parse in B.
			a:   json.RawMessage(`[ 1 , 2 ]`),
			b:   json.RawMessage(`[ 1 , 2 , 3 `),
			err: "unexpected EOF",
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)

			if tc.err != "" {
				_, err := Equal(tc.a, tc.b)
				a.ErrorContains(err, tc.err)
				return
			}

			// Base case.
			if eq, err := Equal(tc.a, tc.b); a.NoError(err) {
				a.Equal(tc.equal, eq)
			}

			// Symmetry case.
			if eq, err := Equal(tc.b, tc.a); a.NoError(err) {
				a.Equal(tc.equal, eq)
			}

			// Reflexive A case.
			if eq, err := Equal(tc.a, tc.a); a.NoError(err) {
				a.True(eq)
			}

			// Reflexive B case.
			if eq, err := Equal(tc.b, tc.b); a.NoError(err) {
				a.True(eq)
			}
		})
	}
}
