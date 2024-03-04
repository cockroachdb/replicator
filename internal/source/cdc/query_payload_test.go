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

package cdc

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
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
		{"insert wrapped",
			`{"after":{"__event__":"insert","pk":42,"v":9},"before":null,"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"v":9}`),
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42]`),
			},
			"",
		},
		{"insert",
			`{"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"v":9}`),
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42]`),
			},
			"",
		},
		{"insert diff",
			`{"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}, "cdc_prev": {"foo":"bar"}}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Before: json.RawMessage(`{"foo":"bar"}`),
				Data:   json.RawMessage(`{"pk":42,"v":9}`),
				Time:   hlc.New(1, 0),
				Key:    json.RawMessage(`[42]`),
			},
			"",
		},
		{"insert wrapped diff",
			`{"after":{"__event__":"insert","pk":42,"v":9},"before":{"pk":42,"v":10},"updated":"1.0"}`,
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
			`{"__event__": "insert", "pk" : 42, "pk2" : 43, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0, ident.New("pk2"), 1),
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"pk2":43,"v":9}`),
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42,43]`),
			},
			"",
		},
		{"update 3 pk",
			`{"__event__": "update", "pk" : 42, "pk2" : 43, "pk3" : 44, "v" : 9, "v2" : 10, "__crdb__": {"updated": "10.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0, ident.New("pk2"), 1, ident.New("pk3"), 2),
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"pk2":43,"pk3":44,"v":9,"v2":10}`),
				Time: hlc.New(10, 0),
				Key:  json.RawMessage(`[42,43,44]`),
			},
			"",
		},
		{"update wrapped",
			`{"after":{"__event__":"update","pk":42,"v":9},"before":{"pk":42,"v":8},"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Data:   json.RawMessage(`{"pk":42,"v":9}`),
				Before: json.RawMessage(`{"pk":42,"v":8}`),
				Time:   hlc.New(1, 0),
				Key:    json.RawMessage(`[42]`),
			},
			"",
		},
		{"delete",
			`{"__event__": "delete", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Data: nil,
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42]`),
			},
			"",
		},
		{"delete wrapped",
			`{"before":{"pk":42,"v":8},"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{
				Before: json.RawMessage(`{"pk":42,"v":8}`),
				Time:   hlc.New(1, 0),
				Key:    json.RawMessage(`[42]`),
			},
			"",
		},
		{"missing pk",
			`{"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0, ident.New("pk2"), 1),
			types.Mutation{},
			`missing primary key: "pk2"`,
		},
		{"missing pk wrapped",
			`{"after":{"__event__":"insert","pk":42,"v":9},"before":null,"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0, ident.New("pk2"), 1),
			types.Mutation{},
			`missing primary key: "pk2"`,
		},
		{"missing timestamp",
			`{"__event__": "delete", "pk" : 42, "v" : 9, "__crdb__": {"something": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			"can't parse timestamp ",
		},
		{"missing timestamp wrapped",
			`{"after":{"__event__":"insert","pk":42,"v":9},"before":null}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			"missing timestamp",
		},
		{"missing op",
			`{"pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			"Add __event__ column to changefeed: CREATE CHANGEFEED ... AS SELECT event_op() AS __event__",
		},
		{"missing op",
			`{"after":{"pk":42,"v":9},"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			"Add __event__ column to changefeed: CREATE CHANGEFEED ... AS SELECT event_op() AS __event__",
		},
		{"invalid op",
			`{"__event__": "select","pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			`unknown __event__ value: "select"`,
		},
		{"invalid op wrapped",
			`{"after":{"__event__":"select","pk":42,"v":9},"before":null,"updated":"1.0"}`,
			ident.MapOf[int](ident.New("pk"), 0),
			types.Mutation{},
			`unknown __event__ value: "select"`,
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
