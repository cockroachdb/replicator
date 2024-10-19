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

	"github.com/cockroachdb/field-eng-powertools/notify"
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

// This can be seen as a skeleton for more interesting sequencers.
func TestStepByStep(t *testing.T) {
	r := require.New(t)
	fixture, err := all.NewFixture(t, time.Minute)
	r.NoError(err)
	ctx := fixture.Context
	fakeTable := ident.NewTable(fixture.TargetSchema.Schema(), ident.New("immediate_table"))

	seqCfg := &sequencer.Config{}
	r.NoError(seqCfg.Preflight())
	seqFixture, err := seqtest.NewSequencerFixture(fixture,
		seqCfg,
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
	bounds.Set(hlc.RangeIncluding(hlc.Zero(), resolved))
	for {
		stat, changed := stats.Get()
		progress := sequencer.CommonProgress(stat)
		if hlc.Compare(progress.Max(), resolved) >= 0 {
			break
		}
		log.Infof("waiting for progress: %s vs %s", progress, resolved)
		select {
		case <-changed:
		case <-ctx.Stopping():
			r.Fail("timed out")
		}
	}
}

func TestImmediate(t *testing.T) {
	seqtest.CheckSequencer(t,
		&all.WorkloadConfig{
			DisableFK:      true,
			DisableStaging: true,
		},
		func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
			return seqFixture.Immediate
		},
		func(t *testing.T, check *seqtest.Check) {})
}
