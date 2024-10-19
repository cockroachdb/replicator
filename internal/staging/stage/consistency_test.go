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

package stage_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/staging/stage"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestConsistency(t *testing.T) {
	stage.EnableSanityChecks()

	r := require.New(t)

	fixture, err := all.NewFixture(t, time.Minute)
	r.NoError(err)

	ctx := fixture.Context
	pool := fixture.StagingPool
	targetDB := fixture.StagingDB.Schema()
	dummyTarget := ident.NewTable(targetDB, ident.New("target"))

	s, err := fixture.Stagers.Get(ctx, dummyTarget)
	r.NoError(err)
	r.NotNil(s)

	// Stage multiple mutations for the same key.
	key := json.RawMessage(`[ 1 ]`)
	muts := []types.Mutation{
		{Key: key, Time: hlc.New(1, 1)},
		{Key: key, Time: hlc.New(2, 2)},
		{Key: key, Time: hlc.New(3, 3)},
		{Key: key, Time: hlc.New(4, 4)},
	}
	r.NoError(s.Stage(ctx, pool, muts))

	// Apply mutation 1; this would create a gap.
	r.ErrorContains(s.MarkApplied(ctx, pool, muts[1:2]), "consistency check failed")

	// Apply mutations 0.
	r.NoError(s.MarkApplied(ctx, pool, muts[0:1]))

	// Apply mutation 2; this would create a gap.
	r.ErrorContains(s.MarkApplied(ctx, pool, muts[2:3]), "consistency check failed")

	// Apply mutation 1 and then 2.
	r.NoError(s.MarkApplied(ctx, pool, muts[1:2]))
	r.NoError(s.MarkApplied(ctx, pool, muts[2:3]))

	// Attempt to re-apply mutation 0 and 1.
	r.ErrorContains(s.MarkApplied(ctx, pool, muts[0:1]), "consistency check failed")
	r.ErrorContains(s.MarkApplied(ctx, pool, muts[1:2]), "consistency check failed")

	// Re-marking the tail isn't an error.
	r.NoError(s.MarkApplied(ctx, pool, muts[2:3]))
}
