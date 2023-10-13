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

package apply

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

func TestMyIdentifier(t *testing.T) {
	tests := []struct {
		name string
		id   ident.Identifier
		want string
	}{
		{
			name: "table",
			id:   ident.NewTable(ident.MustSchema(ident.New("schema")), ident.New("table")),
			want: "`schema`.`table`",
		},
		{
			name: "column",
			id:   ident.New("column"),
			want: "`column`",
		},
		{
			name: "with-dash",
			id:   ident.New("with-dash"),
			want: "`with-dash`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := myIdentifier(tt.id)
			if got != tt.want {
				t.Errorf("myIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}
