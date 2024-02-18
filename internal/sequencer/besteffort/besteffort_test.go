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

package besteffort_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/besteffort"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/seqtest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestAcceptAndSweep(t *testing.T) {
	r := require.New(t)
	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	seqFixture, err := seqtest.NewSequencerFixture(fixture,
		&sequencer.Config{
			Parallelism:     2,
			QuiescentPeriod: 100 * time.Millisecond,
			TimestampLimit:  sequencer.DefaultTimestampLimit,
			SweepLimit:      1000,
		},
		&script.Config{})
	r.NoError(err)

	parentInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (parent INT PRIMARY KEY)")
	r.NoError(err)
	sqlAcceptor := fixture.ApplyAcceptor

	childInfo, err := fixture.CreateTargetTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (
child INT PRIMARY KEY,
parent INT NOT NULL,
val INT DEFAULT 0 NOT NULL,
CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES %s(parent)
)`, parentInfo.Name()))
	r.NoError(err)

	bestEffort := seqFixture.BestEffort
	// We only want BestEffort to do work when told.
	bestEffort.DisableProactive()

	// Sweep the staged mutations into the destination. We expect the
	// entries for {child:1, parent:1} to be applied. The entries
	// pointing to parent:2 will remain queued.
	sweepStopper := stopper.WithContext(ctx)
	defer sweepStopper.Stop(0)
	sweepBounds := &notify.Var[hlc.Range]{}

	group := &types.TableGroup{
		Name:   ident.New(fixture.StagingDB.Raw()),
		Tables: []ident.Table{parentInfo.Name(), childInfo.Name()},
	}

	acc, stats, err := bestEffort.Start(sweepStopper, &sequencer.StartOptions{
		Bounds:   sweepBounds,
		Delegate: types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers),
		Group:    group,
	})
	r.NoError(err)

	// Add child row 1, should queue.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(childInfo.Name(), hlc.New(1, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"child":1,"parent":1,"val":1}`),
				Key:  json.RawMessage(`[1]`),
			},
		}),
		&types.AcceptOptions{},
	))
	peeked, err := fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 1)
	r.Equal(json.RawMessage(`[1]`), peeked[0].Key)

	// Add parent row
	r.NoError(sqlAcceptor.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(parentInfo.Name(), hlc.New(2, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"parent":1}`),
				Key:  json.RawMessage(`[1]`),
				Time: hlc.New(2, 0),
			},
		}),
		&types.AcceptOptions{},
	))
	ct, err := parentInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(1, ct)

	// Add child row 2, should apply immediately.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(childInfo.Name(), hlc.New(3, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"child":2,"parent":1}`),
				Key:  json.RawMessage(`[2]`),
			},
		}),
		&types.AcceptOptions{},
	))
	ct, err = childInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(1, ct)
	// Still expect child row 1 to be staged.
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 1)
	r.Equal(json.RawMessage(`[1]`), peeked[0].Key)

	// Add another child row 1 update, should queue
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(childInfo.Name(), hlc.New(4, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"child":1,"parent":1,"val":4}`),
				Key:  json.RawMessage(`[1]`),
			},
		}),
		&types.AcceptOptions{},
	))
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 2)
	for _, mut := range peeked {
		r.Equal(json.RawMessage(`[1]`), mut.Key)
	}

	// Add a mixed batch of updates, some should queue.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(childInfo.Name(), hlc.New(5, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"child":1,"parent":1,"val":5}`),
				Key:  json.RawMessage(`[1]`),
			},
			{
				Data: json.RawMessage(`{"child":2,"parent":1}`),
				Key:  json.RawMessage(`[2]`),
			},
		}),
		&types.AcceptOptions{},
	))
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 3)
	for _, mut := range peeked {
		r.Equal(json.RawMessage(`[1]`), mut.Key)
	}

	// Add a fast-path batch of mutations, none should queue.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(childInfo.Name(), hlc.New(5, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"child":3,"parent":1}`),
				Key:  json.RawMessage(`[3]`),
			},
			{
				Data: json.RawMessage(`{"child":4,"parent":1}`),
				Key:  json.RawMessage(`[4]`),
			},
		}),
		&types.AcceptOptions{},
	))
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 3)
	for _, mut := range peeked {
		r.Equal(json.RawMessage(`[1]`), mut.Key)
	}

	// Add a multiple batch of mutations that must queue.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(childInfo.Name(), hlc.New(6, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"child":5,"parent":2}`),
				Key:  json.RawMessage(`[5]`),
			},
			{
				Data: json.RawMessage(`{"child":6,"parent":2}`),
				Key:  json.RawMessage(`[6]`),
			},
		}),
		&types.AcceptOptions{},
	))
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 5)

	// Make staged data available to be processed.
	sweepBounds.Set(hlc.Range{hlc.Zero(), hlc.New(5, 1)})
	// Wait until the background process has caught for all tables.
	sweepProgress, swept := stats.Get()
	for {
		progress := sequencer.CommonMin(sweepProgress)
		done := hlc.Compare(progress, hlc.New(5, 1)) >= 0
		if done {
			break
		}
		log.Infof("waiting for sweep progress; currently at %s", progress)
		select {
		case <-swept:
			sweepProgress, swept = stats.Get()
		case <-ctx.Done():
		}
	}
	// This stat is a running total across potentially multiple
	// iterations. It's safe to check Applied, but the number of
	// attempts is unpredictable, although it will always be at least
	// the number of potential records.
	r.Equal(3, sweepProgress.(*besteffort.Stat).Applied)
	r.GreaterOrEqual(sweepProgress.(*besteffort.Stat).Attempted, 3)

	// Examine the remaining staged values. We should see the entries
	// with parent:2.
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 2)
	ct, err = childInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(4, ct)
	// Ensure that the latest value for the {child:1} row was applied.
	r.NoError(fixture.TargetPool.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT val FROM %s WHERE child=1", childInfo.Name())).Scan(&ct))
	r.Equal(5, ct)
}

// TestSweepingFromStaging writes mutations to the staging table and
// then starts the sweep process. This ensures that BestEffort can be
// cold-started. Increasing the number of batches written here is also
// an ersatz performance test of the sweep cycle.
func TestSweepingFromStaging(t *testing.T) {
	const batches = 100

	r := require.New(t)
	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	parentInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (parent INT PRIMARY KEY)")
	r.NoError(err)

	childInfo, err := fixture.CreateTargetTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (
child INT PRIMARY KEY,
parent INT NOT NULL,
val INT DEFAULT 0 NOT NULL,
CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES %s(parent)
)`, parentInfo.Name()))
	r.NoError(err)

	group := &types.TableGroup{
		Name:      ident.New("testing"),
		Enclosing: fixture.TargetSchema.Schema(),
		Tables: []ident.Table{
			parentInfo.Name(),
			childInfo.Name(),
		},
	}

	// Write to staging tables directly, so we're testing the sweeping
	// behavior without measuring the fast-path.
	var ctr int
	parents := make(map[int]struct{})
	children := make(map[int]struct{})
	now := time.Now()
	for i := 0; i < batches; i++ {
		batch := seqtest.GenerateBatch(
			&ctr, hlc.New(int64(i+1), 0),
			parents, children,
			parentInfo.Name(), childInfo.Name())
		for _, sub := range batch.Data {
			r.NoError(sub.Data.Range(func(table ident.Table, data *types.TableBatch) error {
				stager, err := fixture.Stagers.Get(ctx, table)
				if err != nil {
					return err
				}
				return stager.Stage(ctx, fixture.StagingPool, data.Data)
			}))
		}
	}
	log.Infof("staged data in %s", time.Since(now))
	endTime := hlc.New(batches+1, 1)

	// Create sequencer test fixture.
	seqFixture, err := seqtest.NewSequencerFixture(fixture,
		&sequencer.Config{
			Parallelism:     8,
			QuiescentPeriod: 100 * time.Millisecond,
			TimestampLimit:  sequencer.DefaultTimestampLimit,
			SweepLimit:      sequencer.DefaultSweepLimit,
		},
		&script.Config{})
	r.NoError(err)

	// Set up the BestEffort processes.
	bounds := &notify.Var[hlc.Range]{}
	_, stats, err := seqFixture.BestEffort.Start(ctx, &sequencer.StartOptions{
		Bounds:   bounds,
		Delegate: types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers),
		Group:    group,
	})
	r.NoError(err)

	// Set desired range.
	now = time.Now()
	_, _, err = bounds.Update(func(old hlc.Range) (new hlc.Range, _ error) {
		return hlc.Range{old.Min(), endTime}, nil
	})
	r.NoError(err)

	// Wait to catch up.
	for {
		stat, changed := stats.Get()
		min := sequencer.CommonMin(stat)
		if hlc.Compare(min, endTime) >= 0 {
			log.Infof("caught up in %s", time.Since(now))
			break
		}
		log.Infof("waiting for catch-up: %s vs %s", min, endTime)
		select {
		case <-changed:
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}

	// Verify all mutations have been unstaged.
	staged, err := fixture.PeekStaged(ctx, parentInfo.Name(), hlc.Zero(), endTime)
	r.NoError(err)
	r.Empty(staged)
	staged, err = fixture.PeekStaged(ctx, childInfo.Name(), hlc.Zero(), endTime)
	r.NoError(err)
	r.Empty(staged)

	// Verify target row counts against generated data.
	parentCount, err := parentInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(len(parents), parentCount)
	childCount, err := childInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(len(children), childCount)
}
