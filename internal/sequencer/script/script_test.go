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

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/seqtest"
	"github.com/cockroachdb/replicator/internal/sequencer/switcher"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestUserScriptSequencer(t *testing.T) {
	for mode := switcher.MinMode; mode <= switcher.MaxMode; mode++ {
		t.Run(mode.String(), func(t *testing.T) {
			testUserScriptSequencer(t, mode)
		})
	}
}

func testUserScriptSequencer(t *testing.T, baseMode switcher.Mode) {
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
		var schema = fmt.Sprintf(
			`CREATE TABLE %s (k INT PRIMARY KEY, v VARCHAR(2048), ref INT, extras VARCHAR(2048))`,
			tgt)
		_, err := pool.ExecContext(ctx, schema)
		r.NoError(err)
	}
	r.NoError(fixture.Watcher.Refresh(ctx, pool))

	scriptCfg := &script.Config{
		MainPath: "/main.ts",
		FS: &fstest.MapFS{
			"main.ts": &fstest.MapFile{Data: []byte(`
import * as api from "replicator@v1";
api.configureSource("src1", {
  dispatch: (doc, meta) => {
    if (meta.table === undefined) {
      throw "verify meta wiring";
    }
    return {
      "T_1": [ doc ], // Note upper-case table name.
      "t_2": [ doc ]
    };
  },
  deletesTo: (doc, meta) => {
    if (meta.table === undefined) {
      throw "verify meta wiring";
    }
    return { "t_1" : [doc] };
  },
});
api.configureTable("T_1", { // Upper-case table name.
  map: (doc) => {
    doc.v = "cowbell";
    return doc;
  }
});
api.configureTable("t_2", {
  deleteKey: (key: api.DocumentValue[]): api.DocumentValue[] => {
      console.trace("deleteKey() is called before apply()");
      return key;
  },
  map: (doc: api.Document): api.Document => {
      console.trace("map() is called before apply()");
      doc["more_stuff"] = "more_better";
      return doc;
  },
  extras: "extras", // Above property assignment will succeed.
  apply: (ops) => { // Another way of mapping entire batches of data.
    ops = ops.map((op) => {
      op.data.v = "llebwoc";
      return op;
    })
    return api.getTX().apply(ops);
  },
});
`)}}}

	seqCfg := &sequencer.Config{
		Parallelism:     2,
		QuiescentPeriod: 100 * time.Millisecond,
	}
	r.NoError(seqCfg.Preflight())
	seqFixture, err := seqtest.NewSequencerFixture(fixture,
		seqCfg,
		scriptCfg)
	r.NoError(err)
	// Fake timestamps in use.
	seqFixture.BestEffort.SetTimeSource(hlc.Zero)

	base, err := seqFixture.SequencerFor(ctx, baseMode)
	r.NoError(err)

	bounds := &notify.Var[hlc.Range]{}
	wrapped, err := seqFixture.Script.Wrap(ctx, base)
	r.NoError(err)
	acc, stats, err := wrapped.Start(ctx, &sequencer.StartOptions{
		Bounds:   bounds,
		Delegate: types.OrderedAcceptorFrom(fixture.ApplyAcceptor, fixture.Watchers),
		Group: &types.TableGroup{
			Enclosing: fixture.TargetSchema.Schema(),
			Name:      ident.New("src1"), // Aligns with configureSource() call.
			Tables:    tgts,
		},
	})
	r.NoError(err)

	const numEmits = 100
	endTime := hlc.New(numEmits+1, 0)
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
	bounds.Set(hlc.RangeIncluding(hlc.Zero(), endTime))

	// Wait for (async) replication for the first table name.
	progress, progressMade := stats.Get()
	for {
		targetProgress := progress.Progress().GetZero(tgts[0])
		if hlc.Compare(targetProgress.MaxInclusive(), endTime) >= 0 {
			break
		}
		log.Infof("waiting for %s, saw %s", endTime, targetProgress)
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

		// https://github.com/cockroachdb/replicator/issues/689
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

	// Verify that deletes are routed to the correct table.
	endTime = hlc.New(1000*(numEmits+1), 0)
	for i := 0; i < numEmits; i++ {
		r.NoError(acc.AcceptTableBatch(ctx,
			sinktest.TableBatchOf(
				tgts[0],
				hlc.New(1000*int64(i+1), 0), // +1 since zero time is rejected.
				[]types.Mutation{
					{
						Key: json.RawMessage(fmt.Sprintf(`[ %d ]`, i)),
					},
				},
			),
			&types.AcceptOptions{}))
	}
	// Make next batch of mutations eligible for processing.
	bounds.Set(hlc.RangeIncluding(hlc.Zero(), endTime))

	// Wait for (async) replication for the first table name.
	progress, progressMade = stats.Get()
	for {
		targetProgress := progress.Progress().GetZero(tgts[0])
		if hlc.Compare(targetProgress.MaxInclusive(), endTime) >= 0 {
			break
		}
		log.Infof("waiting for %s, saw %s", endTime, targetProgress)
		select {
		case <-progressMade:
			progress, progressMade = stats.Get()
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}

	// Ensure that the values were deleted from the target table.
	var count int
	r.NoError(pool.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s", tgts[0])).Scan(&count))
	r.Zero(count)
}
