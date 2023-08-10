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

package cmap

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqual(t *testing.T) {
	mapper := Mapper[string, string](strings.ToLower)

	tcs := []struct {
		a        Map[string, rune]
		b        Map[string, rune]
		expected bool
	}{
		{
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			a:        NewOf[string, string, rune](mapper, "a", 'a'),
			b:        nil,
			expected: false,
		},
		{
			a:        nil,
			b:        NewOf[string, string, rune](mapper, "a", 'a'),
			expected: false,
		},
		{
			a:        NewOf[string, string, rune](mapper, "a", 'a'),
			b:        NewOf[string, string, rune](mapper, "a", 'a'),
			expected: true,
		},
		{
			a:        NewOf[string, string, rune](mapper, "a", 'a'),
			b:        NewOf[string, string, rune](mapper, "a", 'a', "b", 'b'),
			expected: false,
		},
		{
			a:        NewOf[string, string, rune](mapper, "a", 'a'),
			b:        NewOf[string, string, rune](mapper, "b", 'b'),
			expected: false,
		},
		{
			a:        NewOf[string, string, rune](mapper, "a", 'a'),
			b:        NewOf[string, string, rune](mapper, "A", 'a'),
			expected: true,
		},
		{
			a:        NewOf[string, string, rune](mapper, "a", 'a'),
			b:        NewOf[string, string, rune](mapper, "A", 'b'),
			expected: false,
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			result := Equal(tc.a, tc.b, func(a, b rune) bool {
				return a == b
			})
			assert.Equal(t, result, tc.expected)
		})
	}
}
