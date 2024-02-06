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

package script

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyOps verifies that we can roundtrip from Mutations to applyOps
func TestApplyOps(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		muts []types.Mutation
	}{
		{
			"insert",
			[]types.Mutation{
				{
					Before: nil,
					Data:   []byte(`{"pk":2,"value":"two"}`),
					Key:    []byte(`[2]`),
					Meta:   map[string]any{"insert": 1},
					Time:   hlc.New(int64(time.Now().Nanosecond()), 0),
				},
				{
					Before: nil,
					Data:   []byte(`{"pk":3,"value":"three"}`),
					Key:    []byte(`[3]`),
					Meta:   map[string]any{"insert": 1},
					Time:   hlc.New(int64(time.Now().Nanosecond()), 0),
				},
				{
					Before: []byte(`{"pk":3,"value":"three"}`),
					Data:   nil,
					Key:    []byte(`[3]`),
					Meta:   map[string]any{"delete": 1},
					Time:   hlc.New(int64(time.Now().Nanosecond()), 0),
				},
			},
		},
	}
	for _, tt := range tests {
		r := require.New(t)
		a := assert.New(t)
		t.Run(tt.name, func(t *testing.T) {
			ops, err := applyOpsFromMuts(ctx, tt.muts)
			r.NoError(err)
			muts, err := mutsFromApplyOps(ctx, ops)
			r.NoError(err)
			r.Equal(len(tt.muts), len(muts))
			for idx, mut := range tt.muts {
				if mut.Before == nil {
					a.Nil(muts[idx].Before)
				} else {
					a.JSONEq(string(mut.Before), string(muts[idx].Before))
				}
				if mut.Data == nil {
					a.Nil(muts[idx].Data)
				} else {
					a.JSONEq(string(mut.Data), string(muts[idx].Data))
				}
				if mut.Key == nil {
					a.Nil(muts[idx].Key)
				} else {
					a.JSONEq(string(mut.Key), string(muts[idx].Key))
				}
				r.Equal(mut.Time, muts[idx].Time)
				r.Equal(mut.Meta, muts[idx].Meta)
			}
		})
	}
}
