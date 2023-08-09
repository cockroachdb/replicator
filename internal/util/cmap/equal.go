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

import "github.com/pkg/errors"

var errStop = errors.New("sentinel")

// Comparator returns a comparator for comparable types.
func Comparator[C comparable]() func(C, C) bool {
	return func(a C, b C) bool {
		return a == b
	}
}

// Equal returns true if the two maps have an identical canonical keyset
// and the comparator function returns true for each pairwise key-value
// mapping between the two maps.
func Equal[K, V any](a, b Map[K, V], comparator func(V, V) bool) bool {
	// Nil-nil or identity case
	if a == b {
		return true
	}
	// XOR
	if (a == nil) != (b == nil) {
		return false
	}
	if a.Len() != b.Len() {
		return false
	}

	ret := true
	_ = a.Range(func(aK K, aV V) error {
		bV, ok := b.Get(aK)
		if !ok {
			ret = false
			return errStop
		}
		if !comparator(aV, bV) {
			ret = false
			return errStop
		}
		return nil
	})

	return ret
}
