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

package immediate_test

import (
	"encoding/json"
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

// This can be seen as a skeleton for more interesting sequencers.
func TestImmediate(t *testing.T) {
	r := require.New(t)
	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context
	fakeTable := ident.NewTable(fixture.TargetSchema.Schema(), ident.New("immediate_table"))

	seqFixture, err := seqtest.NewSequencerFixture(fixture,
		&sequencer.Config{
			Parallelism:     2,
			QuiescentPeriod: time.Second,
			SweepLimit:      1000,
		},
		&script.Config{})
	r.NoError(err)
	imm := seqFixture.Immediate

	bounds := &notify.Var[hlc.Range]{}
	acc, stats, err := imm.Start(ctx, &sequencer.StartOptions{
		Bounds:   bounds,
		Delegate: types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers),
		Group: &types.TableGroup{
			Name:   ident.New("immediate_group"),
			Tables: []ident.Table{fakeTable},
		},
	})
	r.NoError(err)

	parentInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (parent INT PRIMARY KEY)")
	r.NoError(err)

	// Add child row 1, should be written to staging.
	r.NoError(acc.AcceptTableBatch(ctx,
		sinktest.TableBatchOf(parentInfo.Name(), hlc.New(1, 0), []types.Mutation{
			{
				Data: json.RawMessage(`{"parent":1}`),
				Key:  json.RawMessage(`[1]`),
			},
		}),
		&types.AcceptOptions{},
	))

	ct, err := parentInfo.RowCount(ctx)
	r.NoError(err)
	r.Equal(1, ct)

	// Ensure table progress tracks resolved timestamps.
	resolved := hlc.New(100, 100)
	bounds.Set(hlc.Range{hlc.Zero(), resolved})
	for {
		stat, changed := stats.Get()
		if stat.Progress().GetZero(fakeTable) == resolved {
			break
		}
		select {
		case <-changed:
		case <-ctx.Stopping():
			r.Fail("timed out")
		}
	}
}
