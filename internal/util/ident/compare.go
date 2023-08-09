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

// Compare returns a value less than, equal to, or greater than zero if
// a should sort before, equal, or after b.
func Compare(a, b Identifier) int {
	aParts := a.Idents(make([]Ident, 0, maxArrayLength+1))
	aLen := len(aParts)
	bParts := b.Idents(make([]Ident, 0, maxArrayLength+1))
	bLen := len(bParts)

	for i := 0; i < aLen && i < bLen; i++ {
		aPart := aParts[i]
		bPart := bParts[i]

		if c := strings.Compare(aPart.Canonical().Raw(), bPart.Canonical().Raw()); c != 0 {
			return c
		}
	}
	return aLen - bLen
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
