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

package script

import (
	"github.com/cockroachdb/cdc-sink/internal/util/crep"
	"github.com/dop251/goja"
)

// The maximum safe numeric value in JavaScript.
const maxInt = 1 << 53

// safeValue returns a JS runtime value that contains the given value
// as though it had been stringified and then parsed.
//
// Numbers will be converted to a string representation to ensure
// minimum loss of fidelity when round-tripped through the userscript.
// If the user actually wants to perform math in JavaScript, the Number
// API is available, or the JS idiom of `+value` can be used.
//
// Goja does not, yet, have builtin support for BigInt. If and when this
// happens, we should revisit this code to emit the numbers as such.
func safeValue(rt *goja.Runtime, value any) (goja.Value, error) {
	if jsVal, ok := value.(goja.Value); ok {
		return jsVal, nil
	}
	canon, err := crep.Canonical(value)
	if err != nil {
		return nil, err
	}
	return rt.ToValue(canon), nil
}
