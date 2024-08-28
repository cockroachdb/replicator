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

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/seqtest"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestStepByStep(t *testing.T) {
	r := require.New(t)
	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	seqCfg := &sequencer.Config{
		Parallelism:     16,
		QuiescentPeriod: 100 * time.Millisecond,
	}
	r.NoError(seqCfg.Preflight())
	seqFixture, err := seqtest.NewSequencerFixture(fixture,
		seqCfg,
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
	bestEffort.SetTimeSource(hlc.Zero)

	// Sweep the staged mutations into the destination. We expect the
	// entries for {child:1, parent:1} to be applied. The entries
	// pointing to parent:2 will remain queued.
	sweepStopper := stopper.WithContext(ctx)
	defer sweepStopper.Stop(0)
	sweepBounds := &notify.Var[hlc.Range]{}

	group := &types.TableGroup{
		Name:      ident.New(fixture.StagingDB.Raw()),
		Enclosing: fixture.TargetSchema.Schema(),
		Tables:    []ident.Table{parentInfo.Name(), childInfo.Name()},
	}

	seq, err := bestEffort.Wrap(ctx, seqFixture.Core)
	r.NoError(err)
	acc, stats, err := seq.Start(sweepStopper, &sequencer.StartOptions{
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
	maxBounds := hlc.RangeIncluding(hlc.Zero(), hlc.New(100, 0))
	peeked, err := fixture.PeekStaged(ctx, childInfo.Name(), maxBounds)
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
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), maxBounds)
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
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), maxBounds)
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
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), maxBounds)
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
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), maxBounds)
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
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), maxBounds)
	r.NoError(err)
	r.Len(peeked, 5)

	// Make staged data available to be processed.
	sweepBounds.Set(hlc.RangeIncluding(hlc.Zero(), hlc.New(5, 0)))
	// Wait until the background process has caught for all tables.
	sweepProgress, swept := stats.Get()
	for {
		progress := sequencer.CommonProgress(sweepProgress)
		done := hlc.Compare(progress.Max(), hlc.New(5, 0)) >= 0
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

	// Examine the remaining staged values. We should see the entries
	// with parent:2.
	peeked, err = fixture.PeekStaged(ctx, childInfo.Name(), maxBounds)
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

func TestBestEffort(t *testing.T) {
	seqtest.CheckSequencer(t, &all.WorkloadConfig{},
		func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
			// We only want BestEffort to do work when told; the test
			// rig uses a counter to generate timestamps.
			seqFixture.BestEffort.SetTimeSource(hlc.Zero)
			next, err := seqFixture.BestEffort.Wrap(fixture.Context, seqFixture.Core)
			require.NoError(t, err)
			return next
		},
		func(t *testing.T, check *seqtest.Check) {})
}
