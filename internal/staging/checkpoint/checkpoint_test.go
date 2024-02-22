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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	log "github.com/sirupsen/logrus"
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
	const maxNanos = int64(1_000)

	t.Run("advance-and-bootstrap", func(t *testing.T) {
		r := require.New(t)
		start := time.Now()
		for i := minNanos; i <= maxNanos; i++ {
			r.NoError(g1.Advance(ctx, hlc.New(i, 0)))

			// Accept cases where a duplicate message is received, as
			// long as it's the tail.
			r.NoError(g1.Advance(ctx, hlc.New(i, 0)))
		}
		log.Infof("inserted rows in %s", time.Since(start))

		// Fast refresh of other group.
		g2.Refresh()

		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(0, 0), hlc.New(minNanos, 1)},
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(0, 0), hlc.New(minNanos, 1)},
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
			hlc.Range{hlc.New(maxNanos/2, 1), hlc.New(maxNanos/2+1, 1)},
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(maxNanos/2, 1), hlc.New(maxNanos/2+1, 1)},
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
			hlc.Range{hlc.New(maxNanos, 1), hlc.New(maxNanos, 1)},
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			// 1 because end is exclusive.
			hlc.Range{hlc.New(maxNanos, 1), hlc.New(maxNanos, 1)},
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

func TestTransitions(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	bounds := &notify.Var[hlc.Range]{}
	expect := func(low, high int) {
		var lo, hi hlc.Time
		if low > 0 {
			lo = hlc.New(int64(low), low+1)
		}
		if high > 0 {
			hi = hlc.New(int64(high), high+1)
		}
		rng := hlc.Range{lo, hi}
		log.Infof("waiting for %s", rng)
		r.NoError(stopvar.WaitForValue(ctx, rng, bounds))
	}

	group, err := fixture.Checkpoints.Start(ctx,
		&types.TableGroup{Name: ident.New("fake")},
		bounds,
	)
	r.NoError(err)
	advance := func(ts int) {
		r.NoError(group.Advance(ctx, hlc.New(int64(ts), ts)))
	}
	commit := func(ts int) {
		h := hlc.New(int64(ts), ts)
		r.NoError(group.Commit(ctx, hlc.Range{h, h.Next()}))
	}

	expect(0, 0)

	// Add a timestamp and expect the exclusive end point to advance
	// beyond the end time.
	advance(2)
	expect(0, 2)

	// Commit the timestamp and expect an empty range that does not
	// overlap the committed time.
	commit(2)
	expect(2, 2)

	// Extend the end time, the min bound should not change.
	advance(5)
	expect(2, 5)

	// Same test as committing 2 above.
	commit(5)
	expect(5, 5)

	// Stack up additional timestamps that we'll incrementally commit.
	// We expect to advance incrementally as the timestamps are
	// committed.
	advance(10)
	advance(15)
	advance(20)
	advance(25)

	expect(5, 10)
	commit(10)
	expect(10, 15)
	commit(15)
	expect(15, 20)

	// Commit several and wait for catch up.
	commit(20)
	commit(25)
	expect(25, 25)

	// We do have guards for ensuring that a resolved timestamp always
	// goes forward, however we do want to verify that we get reasonable
	// answers if there's a gap in the history.
	advance(100)
	advance(105)
	advance(110)
	commit(100)
	commit(110)
	expect(100, 105)
	commit(105)
	expect(110, 110)
}
