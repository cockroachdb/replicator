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

package sequtil_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/sequtil"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is a smoke test to ensure that we can coalesce mutations without
// losing anything along the way.
func TestCopier(t *testing.T) {
	const mutCount = 1000
	const txSize = 11      // mutations per timestamp
	const fragmentSize = 3 // use fragmented reads from db
	const flushSize = 7    // ideal size for coalesced data
	r := require.New(t)
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	table := ident.NewTable(ident.MustSchema(ident.New("schema")), ident.New("fake"))

	bounds := notify.VarOf(hlc.RangeEmpty())
	stagingReader, err := fixture.Stagers.Read(ctx, &types.StagingQuery{
		Bounds:       bounds,
		FragmentSize: fragmentSize,
		Group: &types.TableGroup{
			Enclosing: fixture.TargetSchema.Schema(),
			Name:      ident.New("testing"),
			Tables:    []ident.Table{table},
		},
	})
	r.NoError(err)

	now := time.Now()
	testData := &types.MultiBatch{}
	for i := 0; i < mutCount; i++ {
		r.NoError(testData.Accumulate(table, types.Mutation{
			Data: json.RawMessage("ignored"),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, i)),
			Time: hlc.New(int64(1+i/txSize), 0),
		}))
	}

	// Apply data to the staging table.
	for _, temporal := range testData.Data {
		r.NoError(temporal.Data.Range(func(table ident.Table, batch *types.TableBatch) error {
			stager, err := fixture.Stagers.Get(ctx, table)
			if err != nil {
				return err
			}
			return stager.Stage(ctx, fixture.StagingPool, batch.Data)
		}))
	}
	log.Infof("staged data in %s", time.Since(now))
	now = time.Now()
	endTime := hlc.New(mutCount+1, 1)
	bounds.Set(hlc.RangeIncluding(hlc.Zero(), endTime))

	seen := &types.MultiBatch{}

	copier := &sequtil.Copier{
		Config: &sequencer.Config{
			FlushSize: flushSize,
		},
		Source: stagingReader,
		Each: func(ctx *stopper.Context, batch *types.TemporalBatch, segment bool) error {
			a.LessOrEqual(batch.Count(), fragmentSize)
			return nil
		},
		Flush: func(ctx *stopper.Context, batch *types.MultiBatch, segment bool) error {
			return batch.CopyInto(seen)
		},
		Progress: func(ctx *stopper.Context, progress hlc.Range) error {
			if progress.MaxInclusive() == endTime {
				ctx.Stop(0)
			}
			return nil
		},
	}
	r.NoError(copier.Run(ctx))
	a.Equal(types.Flatten[*types.MultiBatch](testData), types.Flatten[*types.MultiBatch](seen))

	log.Infof("read data in %s", time.Since(now))
}
