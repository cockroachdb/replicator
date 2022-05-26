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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveExponent(t *testing.T) {
	tcs := []struct{ data, expected json.Number }{
		{"", ""},
		{"1", "1"},
		{"4E2", "400"},
		{"4E+2", "400"},
		{"4E-2", "0.04"},
		{"4.321E10", "43210000000"},
		{"not a number", "not a number"},
	}

	for _, tc := range tcs {
		t.Run(string(tc.data), func(t *testing.T) {
			a := assert.New(t)
			a.Equal(tc.expected, removeExponent(tc.data))
		})
	}
}
