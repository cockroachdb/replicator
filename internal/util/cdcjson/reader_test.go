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
	"strings"
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/stretchr/testify/assert"
)

func TestBulkMutationReader(t *testing.T) {
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
			got, err := BulkMutationReader()(strings.NewReader(tt.request))
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(tt.want, got)
		})
	}
}
