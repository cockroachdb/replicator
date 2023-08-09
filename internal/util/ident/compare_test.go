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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	tcs := []struct {
		a, b Identifier
		c    int
	}{
		// Zero-case.
		{
			a: nil,
			b: nil,
			c: 0,
		},
		{
			a: Identifier(nil),
			b: Identifier(nil),
			c: 0,
		},
		{
			a: empty,
			b: empty,
			c: 0,
		},
		{
			a: nil,
			b: empty,
			c: 0,
		},
		// Comparison to empty.
		{
			a: nil,
			b: New("foo"),
			c: -1,
		},
		{
			a: Identifier(nil),
			b: New("foo"),
			c: -1,
		},
		{
			a: empty,
			b: New("foo"),
			c: -1,
		},
		// Lexical ordering.
		{
			a: New("bar"),
			b: New("foo"),
			c: -1,
		},
		// Case-insensitivity.
		{
			a: New("foo"),
			b: New("Foo"),
			c: 0,
		},
		// Multi-part.
		{
			a: NewTable(MustSchema(New("db"), Public), New("foo")),
			b: NewTable(MustSchema(New("DB"), Public), New("FOO")),
			c: 0,
		},
		// Differing lengths.
		{
			a: MustSchema(New("db"), Public),
			b: NewTable(MustSchema(New("DB"), Public), New("FOO")),
			c: -1,
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			a.Equal(tc.c, Compare(tc.a, tc.b))
			// Test symmetry.
			a.Equal(-tc.c, Compare(tc.b, tc.a))
			// Comparator (and Equal).
			a.Equal(tc.c == 0, Comparator[Identifier]()(tc.a, tc.b))
		})
	}
}
