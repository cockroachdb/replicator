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
	"fmt"
	"testing"

	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/seqtest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestBestEffort(t *testing.T) {
	log.SetLevel(log.TraceLevel)
	tcs := []*all.WorkloadConfig{
		{},                // Verify grouped-table behaviors.
		{DisableFK: true}, // Verify single-table behaviors.
	}

	for idx, cfg := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			seqtest.CheckSequencer(t, cfg,
				func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
					// We only want BestEffort to do work when told; the test
					// rig uses a counter to generate timestamps.
					seqFixture.BestEffort.SetTimeSource(hlc.Zero)
					next, err := seqFixture.Staging.Wrap(fixture.Context, seqFixture.Core)
					require.NoError(t, err)
					next, err = seqFixture.BestEffort.Wrap(fixture.Context, next)
					require.NoError(t, err)
					return next
				},
				func(t *testing.T, check *seqtest.Check) {})
		})
	}
}

// This test verifies that the router configuration will respond to
// changes in the underlying schema.
//func TestSchemaChange(t *testing.T) {
//	r := require.New(t)
//	fixture, err := all.NewFixture(t)
//	r.NoError(err)
//	ctx := fixture.Context
//
//	seqCfg := &sequencer.Config{
//		QuiescentPeriod: 100 * time.Millisecond,
//		Parallelism:     1,
//	}
//	r.NoError(seqCfg.Preflight())
//	seqFixture, err := seqtest.NewSequencerFixture(fixture,
//		seqCfg,
//		&script.Config{})
//	r.NoError(err)
//
//	seq, err := seqFixture.BestEffort.Wrap(ctx, seqFixture.Immediate)
//	r.NoError(err)
//
//	// Spy on internal state of the router.
//	schemaChanged := seq.(interface{ SchemaChanged() *notify.Var[struct{}] }).SchemaChanged()
//	_, didChange := schemaChanged.Get()
//
//	// Start the sequencer before creating the table.
//	rec := &recorder.Recorder{}
//	acc, _, err := seq.Start(ctx, &sequencer.StartOptions{
//		Bounds:   notify.VarOf(hlc.RangeEmpty()),
//		Delegate: rec,
//		Group: &types.TableGroup{
//			Name:      ident.New("test"),
//			Enclosing: fixture.TargetSchema.Schema(),
//		},
//	})
//	r.NoError(err)
//
//	tableInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY)")
//	r.NoError(err)
//
//	// Wait for schema change to propagate.
//	select {
//	case <-didChange:
//		_, didChange = schemaChanged.Get()
//	case <-ctx.Done():
//		r.NoError(ctx.Err())
//	}
//
//	r.NoError(acc.AcceptTableBatch(ctx,
//		&types.TableBatch{
//			Table: tableInfo.Name(),
//			Data: []types.Mutation{
//				{
//					Data: json.RawMessage(`{"pk":1}`),
//					Key:  json.RawMessage(`[1]`),
//					Time: hlc.New(1, 1),
//				},
//			},
//		},
//		&types.AcceptOptions{},
//	))
//
//	// Ensure the mutation went through the stack.
//	r.Equal(1, rec.Count())
//
//	// Verify dropping tables.
//	r.NoError(tableInfo.DropTable(ctx))
//	r.NoError(fixture.Watcher.Refresh(ctx, fixture.TargetPool))
//
//	// Wait for schema change to propagate.
//	select {
//	case <-didChange:
//	case <-ctx.Done():
//		r.NoError(ctx.Err())
//	}
//
//	r.ErrorContains(
//		acc.AcceptTableBatch(ctx,
//			&types.TableBatch{
//				Table: tableInfo.Name(),
//				Data: []types.Mutation{
//					{
//						Data: json.RawMessage(`{"pk":1}`),
//						Key:  json.RawMessage(`[1]`),
//						Time: hlc.New(1, 1),
//					},
//				},
//			},
//			&types.AcceptOptions{}),
//		fmt.Sprintf("unknown table %s", tableInfo.Name()))
//}
