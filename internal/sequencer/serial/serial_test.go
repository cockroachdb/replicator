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

package serial_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/seqtest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/stretchr/testify/require"
)

func TestStepByStep(t *testing.T) {
	r := require.New(t)
	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	seqFixture, err := seqtest.NewSequencerFixture(fixture,
		&sequencer.Config{
			Parallelism:     2, // Has no particular effect on serial.
			QuiescentPeriod: time.Second,
			SweepLimit:      sequencer.DefaultSweepLimit,
			TimestampLimit:  sequencer.DefaultTimestampLimit,
		},
		&script.Config{})
	r.NoError(err)
	serial := seqFixture.Serial

	parentInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (parent INT PRIMARY KEY)")
	r.NoError(err)

	childInfo, err := fixture.CreateTargetTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (
child INT PRIMARY KEY,
parent INT NOT NULL REFERENCES %s,
val INT DEFAULT 0 NOT NULL
)`, parentInfo.Name()))
	r.NoError(err)

	// Start the sweeper goroutines and wait until we arrive at the
	// desired end state.
	// Create a stopper for this sweep process.
	sweepBounds := &notify.Var[hlc.Range]{}
	acc, stats, err := serial.Start(ctx, &sequencer.StartOptions{
		Bounds:   sweepBounds,
		Delegate: types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers),
		Group: &types.TableGroup{
			Name:   ident.New(fixture.TargetSchema.Raw()),
			Tables: []ident.Table{parentInfo.Name(), childInfo.Name()},
		},
	})
	r.NoError(err)

	// Add child row 1, should be written to staging.
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

	// Add parent row 1, should also be written to staging.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(parentInfo.Name(), hlc.New(1, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"parent":1}`),
				Key:  json.RawMessage(`[1]`),
			},
		}),
		&types.AcceptOptions{},
	))
	peeked, err = fixture.PeekStaged(ctx, parentInfo.Name(), hlc.Zero(), hlc.New(100, 0))
	r.NoError(err)
	r.Len(peeked, 1)

	group := &types.TableGroup{
		Name: ident.New(fixture.StagingDB.Raw()),
		// These table names are reversed to ensure that we'll re-order
		// based on schema dependency order.
		Tables: []ident.Table{
			childInfo.Name(),
			parentInfo.Name(),
		},
	}

	// Set the sweep bounds here.
	end := hlc.New(100, 0)
	sweepBounds.Set(hlc.Range{hlc.Zero(), end}) // Max is exclusive.

	// Wait for all tables to catch up to the end value.
	sweepProgress, swept := stats.Get()
	for {
		done := true
		for _, tbl := range group.Tables {
			if sweepProgress.Progress().GetZero(tbl) != end {
				done = false
				break
			}
		}
		if done {
			break
		}
		select {
		case <-swept:
			sweepProgress, swept = stats.Get()
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}

	ct, err := parentInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(1, ct)

	ct, err = childInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(1, ct)

	// Demonstrate that we could still pick up unapplied mutations
	// within the existing bounds should it be necessary.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(parentInfo.Name(), hlc.New(2, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"parent":2}`),
				Key:  json.RawMessage(`[2]`),
			},
		}),
		&types.AcceptOptions{},
	))
	sweepBounds.Notify() // Wake the loop before the timer fires.
	<-swept
	ct, err = parentInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(2, ct)
}

func TestSerial(t *testing.T) {
	seqtest.CheckSequencer(t,
		func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
			return seqFixture.Serial
		},
		func(t *testing.T, check *seqtest.Check) {})
}
