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

import "github.com/pkg/errors"

// Equal returns true if the two values are canonically equal. Two
// values are equal if their Canonical representations are equal.
func Equal(a, b any) (bool, error) {
	// Equal is almost always going to be called on  a leaf value like a
	// string-ish or an object. For leaf values, the Canonical function
	// will have a type-switching fast-path; we'll get a comparable in
	// short order. For objects, however, we have to accumulate all,
	// unordered, key-value pairs in order to be able to make a
	// comparison, or at least decompose one of the objects. At that
	// point, we may as well just implement a walker over the
	// fully-cooked, canonical values.
	aVal, err := Canonical(a)
	if err != nil {
		return false, err
	}
	bVal, err := Canonical(b)
	if err != nil {
		return false, err
	}
	return equal(aVal, bVal), nil
}

func equal(aVal, bVal Value) bool {
	switch ta := aVal.(type) {
	case nil:
		return bVal == nil

	case bool:
		tb, ok := bVal.(bool)
		return ok && ta == tb

	case string:
		tb, ok := bVal.(string)
		return ok && ta == tb

	case []Value:
		tb, ok := bVal.([]Value)
		if !ok || len(ta) != len(tb) {
			return false
		}
		for i := range ta {
			if !equal(ta[i], tb[i]) {
				return false
			}
		}
		return true

	case map[string]Value:
		tb, ok := bVal.(map[string]Value)
		if !ok || len(ta) != len(tb) {
			return false
		}
		for key, aElt := range ta {
			bElt, ok := tb[key]
			if !ok {
				return false
			}
			if !equal(aElt, bElt) {
				return false
			}
		}
		return true

	default:
		// This would only get hit if Canonical starts returning
		// additional value types.
		panic(errors.Errorf("unexpected Value type: %T", ta))
	}
}
