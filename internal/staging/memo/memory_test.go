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

package memo_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/staging/memo"
	"github.com/stretchr/testify/assert"
)

// TestMemory validates the in memory version of types.Memo.
func TestMemory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	memo := &memo.Memory{}
	tests := []struct {
		name     string
		key      string
		expected []byte
		insert   bool
	}{
		{"value", "one", []byte("value"), true},
		{"empty", "two", []byte(""), true},
		{"default", "three", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			if tt.insert {
				err := memo.Put(ctx, nil, tt.key, tt.expected)
				if !a.NoError(err) {
					return
				}
			}
			got, err := memo.Get(ctx, nil, tt.key)
			if a.NoError(err) {
				a.Equal(tt.expected, got)
			}
		})
	}
}
