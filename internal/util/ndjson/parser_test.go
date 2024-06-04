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

package ndjson

import (
	"strings"
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		request string
		want    int
		wantErr string
	}{
		{
			name:    "one",
			request: `{"after" : {"pk" : 42, "v" : 9007199254740995}, "key": [42], "updated": "1.0"}`,
			want:    1,
		},
		{
			name: "two",
			request: `{"before" : {"pk" : 42, "v" : 9007199254740995}, "key": [42],"updated": "2.0"}
{"after" : {"pk" : 42, "v" : 9007199254740995}, "key": [42], "updated": "1.0"}`,
			want: 2,
		},
		{
			name: "delete no key",
			request: `{"before" : {"pk" : 42, "v" : 9007199254740995},"updated": "2.0"}
{"after" : {"pk" : 42, "v" : 9007199254740995}, "key": [42], "updated": "1.0"}`,
			want: 1,
		},
	}
	parser := &Parser{BufferSize: 1000}
	table := ident.NewTable(ident.MustSchema(ident.Public), ident.New("table"))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			got, err := parser.Parse(table, ParseMutation, strings.NewReader(tt.request))
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(tt.want, got.Count())
		})
	}
}

func TestResolved(t *testing.T) {
	tests := []struct {
		name    string
		request string
		want    hlc.Time
		wantErr string
	}{
		{
			name:    "ok",
			request: `{"resolved": "1716390502992757000.0000000000"}`,
			want:    hlc.New(1716390502992757000, 0),
		},
		{
			name:    "no timestamp",
			request: `{"something": "1716390502992757000.0000000000"}`,
			wantErr: "CREATE CHANGEFEED must specify the 'WITH resolved' option",
		},
		{
			name:    "invalid timestamp",
			request: `{"resolved": "not a timestamp" }`,
			wantErr: "can't parse timestamp",
		},
	}
	parser := &Parser{BufferSize: 1000}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			got, err := parser.Resolved(strings.NewReader(tt.request))
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(tt.want, got)
		})
	}
}

func TestParseMutation(t *testing.T) {
	tests := []struct {
		name    string
		request string
		want    types.Mutation
		wantErr string
	}{
		{
			name:    "upsert",
			request: `{"after" : {"pk" : 42, "v" : 9007199254740995}, "key": [42], "updated": "1.0"}`,
			want: types.Mutation{
				Data: []byte(`{"pk" : 42, "v" : 9007199254740995}`),
				Key:  []byte(`[42]`),
				Time: hlc.New(1, 0),
			},
		},
		{
			name:    "delete",
			request: `{"before" : {"pk" : 42, "v" : 9007199254740995}, "key": [42] , "updated": "2.0"}`,
			want: types.Mutation{
				Before: []byte(`{"pk" : 42, "v" : 9007199254740995}`),
				Key:    []byte(`[42]`),
				Time:   hlc.New(2, 0),
			},
		},
		{
			name:    "no timestamp",
			request: `{"before" : {"pk" : 42, "v" : 9007199254740995}, "key": [42]}`,
			wantErr: "CREATE CHANGEFEED must specify the 'WITH updated' option",
		},
		{
			name:    "invalid timestamp",
			request: `{"before" : {"pk" : 42, "v" : 9007199254740995}, "key": [42], "updated": "not a timestamp" }`,
			wantErr: "can't parse timestamp",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			got, err := ParseMutation(strings.NewReader(tt.request))
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(tt.want, got)
		})
	}
}
