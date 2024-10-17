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

package cdcjson

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryPayload(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		keys    *ident.Map[int]
		want    types.Mutation
		wantErr string
	}{
		{"insert",
			`{"after":{"pk":42,"v":9},"before":null,"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"v":9}`),
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42]`),
			},
			"",
		},
		// we are not supporting bare any more.
		{"insert bare",
			`{"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			ErrBareEnvelope.Error(),
		},
		{"update with diff",
			`{"after":{"pk":42,"v":9},"before":{"pk":42,"v":10},"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Before: json.RawMessage(`{"pk":42,"v":10}`),
				Data:   json.RawMessage(`{"pk":42,"v":9}`),
				Time:   hlc.New(1, 0),
				Key:    json.RawMessage(`[42]`),
			},
			"",
		},
		{"insert 2 pk",
			`{"after":{"pk":42,"pk2":43,"v":9}, "updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0, ident.New("pk2"), 1),
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"pk2":43,"v":9}`),
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42,43]`),
			},
			"",
		},
		{"insert 3 pk",
			`{ "after": {"pk" : 42, "pk2" : 43, "pk3" : 44, "v" : 9, "v2" : 10},"updated":"10.0"}`,
			ident.MapOf[int](ident.New("pk"), 0, ident.New("pk2"), 1, ident.New("pk3"), 2),
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"pk2":43,"pk3":44,"v":9,"v2":10}`),
				Time: hlc.New(10, 0),
				Key:  json.RawMessage(`[42,43,44]`),
			},
			"",
		},
		{"delete",
			`{"before":{"pk":42,"v":8},"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Before: json.RawMessage(`{"pk":42,"v":8}`),
				Time:   hlc.New(1, 0),
				Key:    json.RawMessage(`[42]`),
			},
			"",
		},
		{"phantom delete",
			`{"before":null,"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Time: hlc.New(1, 0),
			},
			"",
		},
		{"empty",
			`{"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Time: hlc.New(1, 0),
			},
			"",
		},
		{"missing pk",
			`{"after":{"pk":42,"v":9},"before":null,"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0, ident.New("pk2"), 1),
			types.Mutation{},
			`missing primary key: "pk2"`,
		},
		{"missing timestamp",
			`{"after":{"pk":42,"v":9},"before":null}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			"could not find timestamp in field \"updated\" while attempting to parse envelope=wrapped",
		},
		{"primary key values are present in the key field",
			`{"before":{"pk":42,"v":8},"key":[42], "updated":"1.0"}`,
			// Forcing the key map to be empty so that the Key field in the
			// Mutation struct is populated from the logic for unmarshalling the
			// "key" field in the JSON.
			&ident.Map[int]{},
			types.Mutation{
				Before: json.RawMessage(`{"pk":42,"v":8}`),
				Time:   hlc.New(1, 0),
				Key:    json.RawMessage(`[42]`),
			},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)

			q := &queryPayload{
				keys: tt.keys,
			}
			err := q.UnmarshalJSON([]byte(tt.data))
			if tt.wantErr != "" {
				a.EqualError(err, tt.wantErr)
				return
			}
			r.NoError(err)
			mut, err := q.AsMutation()
			r.NoError(err)
			a.Equal(tt.want, mut)
		})
	}
}
