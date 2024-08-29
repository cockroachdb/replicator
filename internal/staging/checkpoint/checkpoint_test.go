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

package checkpoint

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestResolved(t *testing.T) {
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	chk, err := ProvideCheckpoints(ctx, fixture.StagingPool, fixture.StagingDB)
	r.NoError(err)

	// We're going to test that two independent groups operating on the
	// same name are able to coordinate.
	bounds1 := &notify.Var[hlc.Range]{}
	g1, err := chk.Start(ctx,
		&types.TableGroup{Name: ident.New("fake")},
		bounds1,
	)
	r.NoError(err)

	bounds2 := &notify.Var[hlc.Range]{}
	g2, err := chk.Start(ctx,
		&types.TableGroup{Name: ident.New("fake")},
		bounds2,
	)
	r.NoError(err)
	r.NotSame(g1, g2)

	const minNanos = int64(1)
	const maxNanos = int64(1_000)
	part := ident.New("partition")

	t.Run("advance-and-bootstrap", func(t *testing.T) {
		r := require.New(t)
		start := time.Now()
		for i := minNanos; i <= maxNanos; i++ {
			r.NoError(g1.Advance(ctx, part, hlc.New(i, 0)))

			// Accept cases where a duplicate message is received, as
			// long as it's the tail.
			r.NoError(g1.Advance(ctx, part, hlc.New(i, 0)))
		}
		log.Infof("inserted rows in %s", time.Since(start))

		// Fast refresh of other group.
		g2.Refresh()

		r.NoError(stopvar.WaitForValue(ctx,
			hlc.RangeIncluding(hlc.Zero(), hlc.New(maxNanos, 0)),
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			hlc.RangeIncluding(hlc.Zero(), hlc.New(maxNanos, 0)),
			bounds2,
		))
	})

	t.Run("scan", func(t *testing.T) {
		r := require.New(t)
		found, err := chk.ScanForTargetSchemas(ctx)
		r.NoError(err)
		r.Equal([]ident.Schema{ident.MustSchema(ident.New("fake"))}, found)
	})

	// Commit first half to check partial advancement
	t.Run("record-first-half", func(t *testing.T) {
		r := require.New(t)
		r.NoError(g1.Commit(ctx, hlc.RangeIncluding(hlc.Zero(), hlc.New(maxNanos/2, 0))))

		// Fast refresh of other group.
		g2.Refresh()

		r.NoError(stopvar.WaitForValue(ctx,
			hlc.RangeIncluding(hlc.New(maxNanos/2, 0), hlc.New(maxNanos, 0)),
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			hlc.RangeIncluding(hlc.New(maxNanos/2, 0), hlc.New(maxNanos, 0)),
			bounds2))
	})

	// Use the second group instance.
	t.Run("record-and-refresh", func(t *testing.T) {
		r := require.New(t)
		r.NoError(g2.Commit(ctx, hlc.RangeIncluding(hlc.New(minNanos, 0), hlc.New(maxNanos, 0))))

		// Fast refresh of other group.
		g1.Refresh()

		r.NoError(stopvar.WaitForValue(ctx,
			hlc.RangeIncluding(hlc.New(maxNanos, 0), hlc.New(maxNanos, 0)),
			bounds1,
		))
		r.NoError(stopvar.WaitForValue(ctx,
			hlc.RangeIncluding(hlc.New(maxNanos, 0), hlc.New(maxNanos, 0)),
			bounds2))
	})

	t.Run("scan-empty", func(t *testing.T) {
		r := require.New(t)
		found, err := chk.ScanForTargetSchemas(ctx)
		r.NoError(err)
		r.Len(found, 0)
	})

	t.Run("no-going-back", func(t *testing.T) {
		r := require.New(t)
		r.ErrorContains(g1.Advance(ctx, part, hlc.New(1, 1)), "is going backwards")
	})
}

func TestLimitLookahead(t *testing.T) {
	const minNanos = int64(1)
	const maxNanos = int64(10)
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	chk, err := ProvideCheckpoints(ctx, fixture.StagingPool, fixture.StagingDB)
	r.NoError(err)

	bounds1 := &notify.Var[hlc.Range]{}
	g1, err := chk.Start(ctx,
		&types.TableGroup{Name: ident.New("fake")},
		bounds1,
		LimitLookahead(int(maxNanos/2)),
	)
	r.NoError(err)

	part := ident.New("partition")
	for i := minNanos; i <= maxNanos; i++ {
		r.NoError(g1.Advance(ctx, part, hlc.New(i, 0)))
	}

	// Read halfway through.
	r.NoError(stopvar.WaitForValue(ctx,
		hlc.RangeIncluding(hlc.Zero(), hlc.New(maxNanos/2, 0)),
		bounds1,
	))

	// Update base range
	r.NoError(g1.Commit(ctx, hlc.RangeIncluding(hlc.Zero(), hlc.New(1, 0))))

	// Check boundary condition of marking first timestamp.
	r.NoError(stopvar.WaitForValue(ctx,
		hlc.RangeIncluding(hlc.New(1, 0), hlc.New(maxNanos/2, 0)),
		bounds1,
	))

	// Verify that max does advance.
	r.NoError(g1.Commit(ctx, hlc.RangeIncluding(hlc.Zero(), hlc.New(2, 0))))
	r.NoError(stopvar.WaitForValue(ctx,
		hlc.RangeIncluding(hlc.New(2, 0), hlc.New(maxNanos/2+1, 0)),
		bounds1,
	))

	// Verify all resolved.
	r.NoError(g1.Commit(ctx, hlc.RangeIncluding(hlc.Zero(), hlc.New(maxNanos, 0))))
	r.NoError(stopvar.WaitForValue(ctx,
		hlc.RangeIncluding(hlc.New(maxNanos, 0), hlc.New(maxNanos, 0)),
		bounds1,
	))
}

func TestTransitionsInSinglePartition(t *testing.T) {
	testTransitions(t, 1)
}

func TestTransitionsInMultiplePartitions(t *testing.T) {
	testTransitions(t, 16)
}

func testTransitions(t *testing.T, partitionCount int) {
	t.Helper()
	r := require.New(t)
	lastPart := partitionCount - 1

	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	chk, err := ProvideCheckpoints(ctx, fixture.StagingPool, fixture.StagingDB)
	r.NoError(err)

	partIdent := func(part int) ident.Ident {
		return ident.New(fmt.Sprintf("part_%d", part))
	}

	group := chk.newGroup(
		&types.TableGroup{
			Name:      ident.New("my_group"),
			Enclosing: fixture.TargetSchema.Schema(),
		},
		notify.VarOf(hlc.RangeEmpty()),
		1024,
	)

	expect := func(low, high int) {
		lo := hlc.New(int64(low), low)
		hi := hlc.New(int64(high), high)
		rng := hlc.RangeIncluding(lo, hi)

		found, err := group.refreshQuery(ctx, hlc.Zero())
		r.NoError(err)
		r.Equal(rng, found)
	}

	advance := func(part, ts int) {
		r.NoError(group.Advance(ctx,
			partIdent(part),
			hlc.New(int64(ts), ts)))
	}
	var previousCommit hlc.Time
	commit := func(ts int) {
		h := hlc.New(int64(ts), ts)
		r.NoError(group.Commit(ctx, hlc.RangeIncluding(previousCommit, h)))
		previousCommit = h
	}
	commitExactly := func(ts int) {
		h := hlc.New(int64(ts), ts)
		r.NoError(group.Commit(ctx, hlc.RangeIncluding(h, h)))
	}

	expect(0, 0)

	// Bootstrap case to establish all partitions.
	for part := range partitionCount {
		advance(part, 0)
	}
	expect(0, 0)

	// Ensure that the timestamp doesn't advance until all partitons have advanced.
	for part := range partitionCount {
		advance(part, 2)
		if part == lastPart {
			expect(0, 2)
		} else {
			expect(0, 0)
		}
	}

	// Commit the timestamp and expect an empty range that does not
	// overlap the committed time.
	commit(2)
	expect(2, 2)

	// Extend the end time, the min bound should not change.
	for part := range partitionCount {
		advance(part, 5)
		if part == lastPart {
			expect(2, 5)
		} else {
			expect(2, 2)
		}
	}

	// Same test as committing 2 above.
	commit(5)
	expect(5, 5)

	// Stack up additional timestamps that we'll incrementally commit.
	// We expect to advance incrementally as the timestamps are
	// committed.
	for part := range partitionCount {
		for _, ts := range []int{10, 15, 20, 25} {
			advance(part, ts)
		}
	}

	expect(5, 25)
	commit(10)
	expect(10, 25)
	commit(15)
	expect(15, 25)

	// Commit several and wait for catch up.
	commit(20)
	commit(25)
	expect(25, 25)

	// We do have guards for ensuring that a resolved timestamp always
	// goes forward, however we do want to verify that we get reasonable
	// answers if there's a gap in the history.
	for part := range partitionCount {
		for _, ts := range []int{100, 105, 107, 110} {
			advance(part, ts)
		}
	}

	commit(100)
	commitExactly(110)
	expect(100, 107)
	commitExactly(105)
	expect(105, 107)
	commitExactly(107)
	expect(110, 110)

	// This shouldn't change the final outcome.
	for part := range partitionCount {
		r.NoError(group.Ensure(ctx, []ident.Ident{partIdent(part)}))
	}
	expect(110, 110)

	// As a final check, introduce a new partition and ensure that the
	// window rolls backward and can be rolled forward again.
	another := ident.New("another")
	r.NoError(group.Ensure(ctx, []ident.Ident{another}))
	expect(1, 1)
	r.NoError(group.Advance(ctx, another, hlc.New(110, 110)))
	expect(110, 110)
}
