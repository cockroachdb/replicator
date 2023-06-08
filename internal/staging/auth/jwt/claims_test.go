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

package jwt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClaims(t *testing.T) {
	tcs := []struct {
		json string
		err  string
	}{
		{
			json: `{}`,
			err:  "id field is required",
		},
		{
			json: `{"jti": "foobar"}`,
			err:  "no cdc-sink.schemas defined",
		},
		// Smoke-test a valid claim.
		{
			json: `
{
	"jti": "foobar",
	"https://github.com/cockroachdb/cdc-sink": {
		"schemas": [ ["db", "schema" ]]
	}
}`,
		},
		// As previous, but a malformed ident
		{
			json: `
{
	"jti": "foobar",
	"https://github.com/cockroachdb/cdc-sink": {
		"schemas": [ ["missing" ]]
	}
}`,
			err: "expecting 2 parts, had 1",
		},
		// Bad expiration date
		{
			json: `
{
	"jti": "foobar",
	"exp": 0,
	"https://github.com/cockroachdb/cdc-sink": {
		"schemas": [ ["db", "schema" ]]
	}
}`,
			err: "token is expired",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.json, func(t *testing.T) {
			a := assert.New(t)

			var cl Claims
			if tc.err == "" {
				a.NoError(json.Unmarshal([]byte(tc.json), &cl))
				a.NoError(cl.Valid())
			} else if err := json.Unmarshal([]byte(tc.json), &cl); err != nil {
				a.Regexp(tc.err, err.Error())
			} else {
				err := cl.Valid()
				if a.Error(err) {
					a.Regexp(tc.err, err.Error())
				}
			}
		})
	}
}
