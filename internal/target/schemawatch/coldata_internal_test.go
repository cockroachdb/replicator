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

package schemawatch

import "testing"

// TestMaybeSingleQuote verifies that we are
// single quoting strings correctly.
func TestMaybeSingleQuote(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "already_quoted",
			in:   `'test'`,
			want: `'test'`,
		},
		{
			name: "needs_quotes",
			in:   `test`,
			want: `'test'`,
		},
		{
			name: "starts_with",
			in:   `'test`,
			want: `'''test'`,
		},
		{
			name: "ends_with",
			in:   `test'`,
			want: `'test'''`,
		},
		{
			name: "has_quote",
			in:   `te'st`,
			want: `'te''st'`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := maybeSingleQuote(tt.in); got != tt.want {
				t.Errorf("maybeSingleQuote() = %v, want %v", got, tt.want)
			}
		})
	}
}
