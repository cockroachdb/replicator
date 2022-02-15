// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package feed

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestMarshal(t *testing.T) {
	db := ident.New("db")
	schema := ident.New("schema")

	full := complexFeed(db, schema)

	tcs := []struct {
		source   *Feed
		expected string
	}{
		// An empty document should be reified with non-nil maps.
		{
			source: &Feed{},
			expected: `feeds:
    - name: ""
      database: ""
      schema: ""
      immediate: false
      version: 0
      tables: []
`,
		},
		{
			source: full,
			expected: `feeds:
    - name: my_feed
      database: db
      schema: schema
      immediate: true
      version: 42
      tables:
        - schema: schema
          table: complex_table
          cas:
            - updated_at
            - version
          columns:
            - column: expires_at
              deadline: 1h0m0s
            - column: expr
              expression: incoming_column + 1
            - column: feed_name
              targetColumn: renamed_column
            - column: known_but_boring
            - column: synthetic
              expression: gen_random_uuid()
              synthetic: true
            - column: updated_at
              deadline: 1m0s
            - column: version
        - schema: schema
          table: simple_table
`,
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)

			// Marshal the data to YAML and check the values.
			data, err := yaml.Marshal(envelope{[]*Feed{tc.source}})
			a.NoError(err)
			a.Equal(tc.expected, string(data))

			// Unmarshal the data to a new struct.
			var e envelope
			a.NoError(yaml.Unmarshal(data, &e))

			// Now, re-marshal the values and check equality.
			data, err = yaml.Marshal(envelope{[]*Feed{tc.source}})
			a.NoError(err)
			a.Equal(tc.expected, string(data))
		})
	}
}
