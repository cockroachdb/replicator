// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
