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

package eventproc

import (
	"testing"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestGetTableName(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    ident.Ident
		wantErr error
	}{
		{
			name: "good",
			path: "202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson",
			want: ident.New("mytable"),
		},
		{
			name:    "invalid suffix",
			path:    "202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.csv",
			wantErr: ErrInvalidPath,
		},
		{
			name:    "invalid",
			path:    "-202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.json",
			wantErr: ErrInvalidPath,
		},
		{
			name:    "empty",
			path:    "",
			wantErr: ErrInvalidPath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			got, err := getTableName(tt.path)
			if tt.wantErr != nil {
				a.ErrorIs(err, tt.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(tt.want, got)
		})
	}
}
