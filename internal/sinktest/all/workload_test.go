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

package all

import (
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/stretchr/testify/require"
)

// This writes a batch of mutations directly to the target.
func TestWorkloadSmoke(t *testing.T) {
	r := require.New(t)

	fixture, err := NewFixture(t, time.Minute)
	r.NoError(err)
	ctx := fixture.Context

	w, _, err := fixture.NewWorkload(ctx, &WorkloadConfig{})
	r.NoError(err)

	batch := &types.MultiBatch{}
	for i := range 1024 {
		w.GenerateInto(batch, hlc.New(int64(i+1), i))
	}
	acc := types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers)
	r.NoError(acc.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{}))

	r.True(w.CheckConsistent(ctx, t))
}

// Demonstrate how to create a swapped fixture.
func TestFixtureSwap(t *testing.T) {
	r := require.New(t)

	fixture, err := NewFixture(t, time.Minute)
	r.NoError(err)

	next, err := NewFixtureFromBase(fixture.Swapped(), time.Minute)
	r.NoError(err)
	r.Same(fixture.SourcePool.DB, next.TargetPool.DB)
	r.NotSame(fixture.ApplyAcceptor, next.ApplyAcceptor)
}
