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

package memo_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/stretchr/testify/assert"
)

func TestRoundtrip(t *testing.T) {
	fixture, err := all.NewFixture(t, time.Minute)
	if !assert.NoError(t, err) {
		return
	}

	ctx := fixture.Context
	memo := fixture.Memo
	pool := fixture.StagingPool

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
				err := memo.Put(ctx, pool, tt.key, tt.expected)
				if !a.NoError(err) {
					return
				}
			}
			got, err := memo.Get(ctx, pool, tt.key)
			if a.NoError(err) {
				a.Equal(tt.expected, got)
			}
		})
	}
}
