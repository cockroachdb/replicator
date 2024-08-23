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

package core

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestPoison(t *testing.T) {
	r := require.New(t)

	// The time doesn't really matter, it just needs to be non-zero.
	m1 := types.Mutation{Key: json.RawMessage(`[ 1 ]`), Time: hlc.New(1, 0)}
	m2 := types.Mutation{Key: json.RawMessage(`[ 2 ]`), Time: hlc.New(1, 0)}

	table := ident.NewTable(ident.MustSchema(ident.New("schema")), ident.New("table"))

	b1 := &types.MultiBatch{}
	r.NoError(b1.Accumulate(table, m1))

	b12 := &types.MultiBatch{}
	r.NoError(b12.Accumulate(table, m1))
	r.NoError(b12.Accumulate(table, m2))

	b2 := &types.MultiBatch{}
	r.NoError(b2.Accumulate(table, m2))

	t.Run("smoke", func(t *testing.T) {
		r := require.New(t)

		s := newPoisonSet(2)
		r.True(s.IsClean())
		r.False(s.IsFull())
		r.False(s.IsPoisoned(b1))
		r.False(s.IsPoisoned(b12))
		r.False(s.IsPoisoned(b2))

		s.MarkPoisoned(b1)
		r.False(s.IsClean())
		r.False(s.IsFull())
		r.True(s.IsPoisoned(b1))  // Simple, returns true.
		r.False(s.IsPoisoned(b2)) // Not yet overlapping.

		r.True(s.IsPoisoned(b12)) // Side-effect: Also poisons m2
		r.True(s.IsPoisoned(b2))  // Test contagion.
		r.True(s.IsFull())
	})

	t.Run("individual", func(t *testing.T) {
		r := require.New(t)
		s := newPoisonSet(2)

		// Mark and then remove the mark.
		s.MarkMutation(table, m1)
		r.False(s.IsClean())
		r.False(s.IsFull())

		s.RemoveMark(table, m1)
		r.True(s.IsClean())
		r.False(s.IsFull())

		// Once full, removal should be a no-op.
		s.MarkMutation(table, m1)
		s.MarkMutation(table, m2)
		r.False(s.IsClean())
		r.True(s.IsFull())

		s.RemoveMark(table, m1)
		r.False(s.IsClean())
		r.True(s.IsFull())
	})

	t.Run("merge", func(t *testing.T) {
		r := require.New(t)
		sA := newPoisonSet(5) // Larger than sB to test full flag.
		sB := newPoisonSet(2)

		sA.MarkMutation(table, m1)
		sB.Merge(sA)
		r.False(sB.IsClean())
		r.True(sB.IsMutationPoisoned(table, m1))

		sB.MarkMutation(table, m2)
		r.True(sB.IsFull())

		sA.Merge(sB)
		r.True(sA.IsFull())
	})

	t.Run("full", func(t *testing.T) {
		r := require.New(t)

		s := newPoisonSet(100)
		s.ForceFull()
		r.False(s.IsClean())
		r.True(s.IsFull())
		r.True(s.IsPoisoned(b1))

		// Marking should be a no-op once full.
		s.MarkPoisoned(b1)
		r.Empty(s.mu.data)
	})
}
