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

package apply

import (
	"encoding/json"
	"log"
	"regexp"

	"github.com/cockroachdb/apd"
)

var exponentSuffix = regexp.MustCompile(`[Ee][+-]?\d+$`)

// removeExponent detects instances of json.Number which are formatted
// using an explicit exponent and converts them to a non-exponential
// form. This addresses a minor incompatibility with the numeric formats
// accepted by the pgx type-coercion logic. This only appears to be
// relevant when using DECIMAL columns, where their json representation
// may sometimes look like "4E+2".
func removeExponent(val json.Number) json.Number {
	// It's tempting to just call the Int64 or Float64 methods on the
	// json.Number, but we fall into a trap if the values can't be
	// represented exactly with native types, as is sometimes the case
	// for using the DECIMAL sql types. We'll delegate to the
	// arbitrary-precision-decimal library used by CRDB itself. We also
	// know that the input value parses as a numeric value and matches
	// the above regex, so we can treat any error as a fatal failure.
	if exponentSuffix.Match([]byte(val)) {
		parsed, _, err := apd.NewFromString(val.String())
		if err != nil {
			log.Fatal(err)
		}
		return json.Number(parsed.Text('f'))
	}
	return val
}
