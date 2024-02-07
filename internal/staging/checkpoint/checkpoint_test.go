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

package checkpoint_test

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/stretchr/testify/require"
)

func TestResolved(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	// We're going to test that two independent groups operating on the
	// same name are able to coordinate.
	bounds1 := &notify.Var[hlc.Range]{}
	g1, err := fixture.Checkpoints.Start(ctx,
		&types.TableGroup{Name: ident.New("fake")},
		bounds1,
	)
	r.NoError(err)

	bounds2 := &notify.Var[hlc.Range]{}
	g2, err := fixture.Checkpoints.Start(ctx,
		&types.TableGroup{Name: ident.New("fake")},
		bounds2,
	)
	r.NoError(err)
	r.NotSame(g1, g2)

	const minNanos = int64(1)
	const maxNanos = int64(10)

	t.Run("mark-and-refresh", func(t *testing.T) {
		r := require.New(t)
		for i := minNanos; i <= maxNanos; i++ {
			r.NoError(g1.Advance(ctx, hlc.New(i, 0)))
		}

		// Fast refresh of other group.
		g2.Refresh()

		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(0, 0), hlc.New(maxNanos, 1)},
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(0, 0), hlc.New(maxNanos, 1)},
			bounds2,
		))
	})

	t.Run("scan", func(t *testing.T) {
		r := require.New(t)
		found, err := fixture.Checkpoints.ScanForTargetSchemas(ctx)
		r.NoError(err)
		r.Equal([]ident.Schema{ident.MustSchema(ident.New("fake"))}, found)
	})

	// Commit first half to check partial advancement
	t.Run("record-first-half", func(t *testing.T) {
		r := require.New(t)
		// Setting logical to 1 since end is exclusive; makes the tests below less awkward.
		r.NoError(g1.Commit(ctx, hlc.Range{hlc.New(0, 0), hlc.New(maxNanos/2, 1)}))

		// Fast refresh of other group.
		g2.Refresh()

		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(maxNanos/2, 0), hlc.New(maxNanos, 1)},
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(maxNanos/2, 0), hlc.New(maxNanos, 1)},
			bounds2))
	})

	// Use the second group instance.
	t.Run("record-and-refresh", func(t *testing.T) {
		r := require.New(t)
		// Setting logical to 1 since end is exclusive; makes the tests below less awkward.
		r.NoError(g2.Commit(ctx, hlc.Range{hlc.New(minNanos, 0), hlc.New(maxNanos, 1)}))

		// Fast refresh of other group.
		g1.Refresh()

		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(maxNanos, 0), hlc.New(maxNanos, 1)},
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(maxNanos, 0), hlc.New(maxNanos, 1)},
			bounds2))
	})

	t.Run("scan-empty", func(t *testing.T) {
		r := require.New(t)
		found, err := fixture.Checkpoints.ScanForTargetSchemas(ctx)
		r.NoError(err)
		r.Len(found, 0)
	})

	t.Run("no-going-back", func(t *testing.T) {
		r := require.New(t)
		r.ErrorContains(g1.Advance(ctx, hlc.New(1, 1)), "is going backwards")
	})
}
