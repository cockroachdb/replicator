// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
