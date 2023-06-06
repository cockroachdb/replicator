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

	a := assert.New(t)
	r := require.New(t)
	tests := []struct {
		name    string
		data    string
		keys    map[ident.Ident]int
		want    types.Mutation
		wantErr string
	}{
		{"insert",
			`{"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			map[ident.Ident]int{ident.New("pk"): 0},
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"v":9}`),
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42]`),
			},
			"",
		},
		{"insert 2 pk",
			`{"__event__": "insert", "pk" : 42, "pk2" : 43, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			map[ident.Ident]int{ident.New("pk"): 0, ident.New("pk2"): 1},
			types.Mutation{
				Data: json.RawMessage(`{"pk":42,"pk2":43,"v":9}`),
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42,43]`),
			},
			"",
		},
		{"delete",
			`{"__event__": "delete", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			map[ident.Ident]int{ident.New("pk"): 0},
			types.Mutation{
				Data: nil,
				Time: hlc.New(1, 0),
				Key:  json.RawMessage(`[42]`),
			},
			"",
		},
		{"missing pk",
			`{"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}`,
			map[ident.Ident]int{ident.New("pk"): 0, ident.New("pk2"): 1},
			types.Mutation{},
			`expecting a value for key: "pk2"`,
		},
		{"missing timestamp",
			`{"__event__": "delete", "pk" : 42, "v" : 9, "__crdb__": {"something": "1.0"}}`,
			map[ident.Ident]int{ident.New("pk"): 0},
			types.Mutation{},
			"can't parse timestamp ",
		},
		{"missing op",
			`{"pk" : 42, "v" : 9, "__crdb__": {"something": "1.0"}}`,
			map[ident.Ident]int{ident.New("pk"): 0},
			types.Mutation{},
			"CREATE CHANGEFEED must specify the __event__ colum set to op_event()",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &queryPayload{
				keys: tt.keys,
			}
			err := q.UnmarshalJSON([]byte(tt.data))
			if tt.wantErr != "" {
				a.EqualError(err, tt.wantErr)
				return
			}
			mut, err := q.AsMutation()
			r.NoError(err)
			a.Equal(tt.want, mut)
		})
	}
}
