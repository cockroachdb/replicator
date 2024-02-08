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

package retire_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/seqtest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/stretchr/testify/require"
)

func TestRetire(t *testing.T) {
	r := require.New(t)
	fixture, err := all.NewFixture(t)
	r.NoError(err)
	seqFixture, err := seqtest.NewSequncerFixture(fixture,
		&sequencer.Config{
			Parallelism:     2,
			QuiescentPeriod: time.Second,
			SweepLimit:      1000,
		},
		&script.Config{})
	r.NoError(err)
	ctx := fixture.Context

	tblInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY)")
	r.NoError(err)

	// Start a Retire worker, we'll give it bounds later.
	bounds := &notify.Var[hlc.Range]{}
	progress := seqFixture.Retire.Start(ctx, &types.TableGroup{
		Name:   ident.New("test"),
		Tables: []ident.Table{tblInfo.Name()},
	}, bounds)

	// Stage rows for some table.
	const rowcount = 100
	const unstageStart = 0
	const unstageCount = 25

	muts := make([]types.Mutation, rowcount)
	for idx := range muts {
		muts[idx] = types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "pk" : %d }`, idx)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, idx)),
			Time: hlc.New(int64(idx), idx),
		}
	}

	stager, err := fixture.Stagers.Get(ctx, tblInfo.Name())
	r.NoError(err)
	r.NoError(stager.Store(ctx, fixture.StagingPool, muts))

	// Unstage mutations, marking them as applied.
	unstaged := 0
	_, _, err = fixture.Stagers.Unstage(ctx,
		fixture.StagingPool,
		&types.UnstageCursor{
			StartAt:        hlc.New(unstageStart, 0),
			EndBefore:      hlc.New(unstageStart+unstageCount, 0),
			Targets:        []ident.Table{tblInfo.Name()},
			TimestampLimit: 100,
		},
		func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
			unstaged++
			return nil
		})
	r.NoError(err)
	r.Equal(unstageCount, unstaged)

	// Ensure desired behavior from API and backing table.
	staged, err := fixture.PeekStaged(ctx, tblInfo.Name(), hlc.Zero(), hlc.New(rowcount*2, 0))
	r.NoError(err)
	r.Len(staged, rowcount-unstageCount)

	// Spy on the staging table via backdoor protocol.
	countStaged := func() (ct int) {
		stagingTable := stager.(interface{ GetTable() ident.Table }).GetTable()
		r.NoError(fixture.StagingPool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s", stagingTable),
		).Scan(&ct))
		return
	}
	r.Equal(rowcount, countStaged())

	// Update the retirement bounds and wait for progress.
	bounds.Set(hlc.Range{hlc.New(unstageStart+unstageCount, 0), hlc.New(rowcount*2, 0)})
	r.NoError(stopvar.WaitForValue(ctx, hlc.New(unstageStart+unstageCount, 0), progress))
	r.Equal(rowcount-unstageCount, countStaged())
}
