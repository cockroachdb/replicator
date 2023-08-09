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

import "strings"

// Compare is similar to [strings.Compare]. It returns -1, 0, or 1 if a
// should sort before, equal to, or after b. Nil or empty values sort
// before any non-nil, non-empty value.
func Compare(a, b Identifier) int {
	// Incrementally consume parts until we see a difference.
	var aPart, bPart Ident
	for a != nil && !a.Empty() && b != nil && !b.Empty() {
		aPart, a = a.Split()
		bPart, b = b.Split()

		if c := strings.Compare(aPart.Canonical().Raw(), bPart.Canonical().Raw()); c != 0 {
			return c
		}
	}
	if a == nil || a.Empty() {
		if b == nil || b.Empty() {
			return 0
		}
		return -1
	}
	return 1
}

// Comparator returns a comparison function.
func Comparator[I Identifier]() func(I, I) bool {
	return func(a, b I) bool {
		return Equal(a, b)
	}
}

// Equal returns true if the identifiers are equal without considering
// case.
func Equal(a, b Identifier) bool {
	return Compare(a, b) == 0
}
