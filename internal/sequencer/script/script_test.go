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

package script_test

// This file was adapted from the original logical-package script tests.

import (
	"encoding/json"
	"fmt"
	"testing"
	"testing/fstest"
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

func TestUserScriptSequencer(t *testing.T) {
	r := require.New(t)

	// Create a basic test fixture.
	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	dbName := fixture.TargetSchema.Schema()
	pool := fixture.TargetPool

	// Create some tables.
	tgts := []ident.Table{
		ident.NewTable(dbName, ident.New("t_1")),
		ident.NewTable(dbName, ident.New("t_2")),
	}

	for _, tgt := range tgts {
		var schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v VARCHAR(2048), ref INT)`, tgt)
		_, err := pool.ExecContext(ctx, schema)
		r.NoError(err)
	}
	r.NoError(fixture.Watcher.Refresh(ctx, pool))

	scriptCfg := &script.Config{
		MainPath: "/main.ts",
		FS: &fstest.MapFS{
			"main.ts": &fstest.MapFile{Data: []byte(`
import * as api from "cdc-sink@v1";
api.configureSource("src1", {
  dispatch: (doc) => ({
    "T_1": [ doc ], // Note upper-case table name.
    "t_2": [ doc ]
  }),
  deletesTo: "t_1"
});
api.configureTable("T_1", { // Upper-case table name.
  map: (doc) => {
    doc.v = "cowbell";
    return doc;
  }
});
api.configureTable("t_2", {
  map: (doc) => {
    doc.v = "llebwoc";
    return doc;
  }
});
`)}}}

	seqFixture, err := seqtest.NewSequncerFixture(fixture,
		&sequencer.Config{
			Parallelism:     2,
			QuiescentPeriod: time.Second,
			SweepLimit:      1000,
		},
		scriptCfg)
	r.NoError(err)

	scriptSeq := seqFixture.Script

	bounds := &notify.Var[hlc.Range]{}
	wrapped, err := scriptSeq.Wrap(ctx, seqFixture.Serial)
	r.NoError(err)
	acc, stats, err := wrapped.Start(ctx, &sequencer.StartOptions{
		Bounds:   bounds,
		Delegate: types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers),
		Group: &types.TableGroup{
			Name:   ident.New("src1"), // Aligns with configureSource() call.
			Tables: tgts[:1],          // Only drive from the 0th table
		},
	})
	r.NoError(err)

	const numEmits = 100
	endTime := hlc.New(numEmits+1, 1)
	for i := 0; i < numEmits; i++ {
		r.NoError(acc.AcceptTableBatch(ctx,
			sinktest.TableBatchOf(
				tgts[0],
				hlc.New(int64(i+1), 0), // +1 since zero time is rejected.
				[]types.Mutation{
					{
						Data: json.RawMessage(fmt.Sprintf(`{ "k": %d }`, i)),
						Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, i)),
					},
				},
			),
			&types.AcceptOptions{}))
	}

	// Make staged mutations eligible for processing.
	bounds.Set(hlc.Range{hlc.Zero(), endTime})

	// Wait for (async) replication for the first table name.
	progress, progressMade := stats.Get()
	for {
		if progress.Progress().GetZero(tgts[0]) == endTime {
			break
		}
		select {
		case <-progressMade:
			progress, progressMade = stats.Get()
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}

	// Verify that the script transformed the values.
	for idx, tgt := range tgts {
		var search string
		switch idx {
		case 0:
			search = "cowbell"
		case 1:
			search = "llebwoc"
		}

		// https://github.com/cockroachdb/cdc-sink/issues/689
		var q string
		switch fixture.TargetPool.Product {
		case types.ProductCockroachDB, types.ProductPostgreSQL:
			q = "SELECT count(*) FROM %s WHERE v = $1"
		case types.ProductMariaDB, types.ProductMySQL:
			q = "SELECT count(*) FROM %s WHERE v = ?"
		case types.ProductOracle:
			q = "SELECT count(*) FROM %s WHERE v = :v"
		default:
			r.Fail("unimplemented product")
		}

		var count int
		r.NoError(pool.QueryRowContext(ctx, fmt.Sprintf(q, tgt), search).Scan(&count))
		r.Equalf(numEmits, count, "in table %s", tgt)
	}
}
