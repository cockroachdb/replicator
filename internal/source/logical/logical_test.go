// Copyright 2023 The Cockroach Authors
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

package logical_test

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logicalTestMode struct {
	backfill  bool
	chaos     bool
	immediate bool
	fk        bool
}

func TestLogical(t *testing.T) {
	tcs := []struct {
		name string
		mode *logicalTestMode
	}{
		{
			name: "consistent",
			mode: &logicalTestMode{},
		},
		{
			name: "consistent-backfill",
			mode: &logicalTestMode{
				backfill: true,
			},
		},
		{
			name: "consistent-chaos",
			mode: &logicalTestMode{
				chaos: true,
			},
		},
		{
			name: "consistent-chaos-backfill",
			mode: &logicalTestMode{
				backfill: true,
				chaos:    true,
			},
		},
		{
			name: "fk",
			mode: &logicalTestMode{
				fk: true,
			},
		},
		{
			name: "fk-backfill",
			mode: &logicalTestMode{
				backfill: true,
				fk:       true,
			},
		},
		{
			name: "fk-backfill-chaos",
			mode: &logicalTestMode{
				backfill: true,
				chaos:    true,
				fk:       true,
			},
		},
		{
			name: "fk-chaos",
			mode: &logicalTestMode{
				chaos: true,
				fk:    true,
			},
		},
		{
			name: "immediate",
			mode: &logicalTestMode{
				immediate: true,
			},
		},
		{
			name: "immediate-chaos",
			mode: &logicalTestMode{
				chaos:     true,
				immediate: true,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			testLogicalSmoke(t, tc.mode)
		})
	}
}

func testLogicalSmoke(t *testing.T, mode *logicalTestMode) {
	log.SetLevel(log.TraceLevel)
	a := assert.New(t)

	// Create a basic test fixture.
	fixture, cancel, err := base.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TargetSchema.Schema()
	pool := fixture.TargetPool

	// Create some tables.
	const tableCount = 4
	tgts := make([]ident.Table, tableCount)
	for idx := range tgts {
		tgts[idx] = ident.NewTable(dbName, ident.New(fmt.Sprintf("t%d", idx)))
	}

	// In FK mode, declare tables to enforce ordering.
	for idx, tgt := range tgts {
		var schema string
		if mode.fk && idx > 0 {
			schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v VARCHAR(2048), ref INT REFERENCES %s NOT NULL)`,
				tgt, tgts[idx-1])
		} else {
			schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v VARCHAR(2048), ref INT)`, tgt)
		}
		if _, err := pool.ExecContext(ctx, schema); !a.NoError(err) {
			return
		}
	}

	// Ensure that sorting must happen.
	rand.Shuffle(tableCount, func(i, j int) {
		tgts[i], tgts[j] = tgts[j], tgts[i]
	})

	gen := newGenerator(tgts)
	numEmits := 3 * batches.Size()
	gen.emit(numEmits)

	var chaosProb float32
	if mode.chaos {
		chaosProb = 0.01
	}

	cfg := &logical.BaseConfig{
		ApplyTimeout:       time.Second, // Increase to make using the debugger easier.
		ChaosProb:          chaosProb,
		ForeignKeysEnabled: mode.fk,
		Immediate:          mode.immediate,
		RetryDelay:         time.Nanosecond,
		StagingConn:        fixture.StagingPool.ConnectionString,
		StagingSchema:      fixture.StagingDB.Schema(),
		StandbyTimeout:     5 * time.Millisecond,
		TargetConn:         pool.ConnectionString,
	}
	if mode.backfill {
		cfg.BackfillWindow = time.Minute
	} else {
		cfg.BackfillWindow = 0
	}

	factory, cancelFactory, err := logical.NewFactoryForTests(ctx, cfg)
	if !a.NoError(err) {
		return
	}
	defer cancelFactory()

	loop, cancelLoop, err := factory.Start(&logical.LoopConfig{
		Dialect:      gen,
		LoopName:     "generator",
		TargetSchema: dbName,
	})
	if !a.NoError(err) {
		return
	}

	// Start a goroutine to await the end consistent point.
	endConsistent := make(chan stamp.Stamp, 1)
	go func() {
		defer close(endConsistent)

		for {
			cp, updated := loop.GetConsistentPoint()
			if stamp.Compare(cp, &fakeMessage{Index: numEmits - 1}) >= 0 {
				endConsistent <- cp
				return
			}

			select {
			case <-updated:
			case <-ctx.Done():
				a.Fail("timed out")
				return
			}
		}
	}()

	// Wait for replication.
	for _, tgt := range tgts {
		for {
			var count int
			if err := pool.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tgt)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Tracef("backfill count %d", count)
			if count == numEmits {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Wait for the loop to shut down, or a timeout.
	cancelLoop()
	gen.emit(0) // Kick the simplistic ReadInto loop so that it exits.
	select {
	case <-loop.Stopped():
	case <-time.After(time.Second):
		a.Fail("timed out waiting for shutdown")
	}
	if !mode.chaos && !mode.backfill {
		a.Equal(int32(1), gen.atomic.processExits.Load())
		a.Equal(int32(1), gen.atomic.readIntoExits.Load())
	}

	select {
	case <-endConsistent:
	case <-time.After(time.Second):
		a.Fail("did not find awaited consistent point")
	}

	// Verify that we did drain the generator.
	gen.readIntoMu.Lock()
	defer gen.readIntoMu.Unlock()
	a.Equal(numEmits, gen.readIntoMu.lastBatchSent)

	// Verify that we saw all messages.
	gen.processMu.Lock()
	defer gen.processMu.Unlock()
	// Verify that we saw each unique key at least once. The actual
	// slice of messages will contain repeated entries in the chaos
	// tests.
	found := make(map[int]struct{})
	for _, msg := range gen.processMu.messages {
		if fake, ok := msg.(*fakeMessage); ok {
			found[fake.Index] = struct{}{}
		}
	}
	a.Len(found, numEmits)
}

// TestUserScript injects user-provided logic into a loop.
func TestUserScript(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	// Create a basic test fixture.
	fixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

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

	cfg := &logical.BaseConfig{
		ApplyTimeout:   2 * time.Minute, // Increase to make using the debugger easier.
		Immediate:      false,
		StagingConn:    fixture.StagingPool.ConnectionString,
		StagingSchema:  fixture.StagingDB.Schema(),
		StandbyTimeout: 5 * time.Millisecond,
		TargetConn:     pool.ConnectionString,

		ScriptConfig: script.Config{
			MainPath: "/main.ts",
			FS: &fstest.MapFS{
				"main.ts": &fstest.MapFile{Data: []byte(`
import * as api from "cdc-sink@v1";
api.configureSource("t1", {
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
`)}}}}

	// Create a generator for the upstream names.
	gen := newGenerator([]ident.Table{
		ident.NewTable(dbName, ident.New("t1")),
	})
	const numEmits = 100
	gen.emit(numEmits)

	factory, cancelFactory, err := logical.NewFactoryForTests(ctx, cfg)
	r.NoError(err)
	defer cancelFactory()

	_, cancelLoop, err := factory.Start(&logical.LoopConfig{
		Dialect:      gen,
		LoopName:     "generator",
		TargetSchema: dbName,
	})
	r.NoError(err)
	defer cancelLoop()

	// Wait for replication.
	for idx, tgt := range tgts {
		var search string
		switch idx {
		case 0:
			search = "cowbell"
		case 1:
			search = "llebwoc"
		}
		for {
			var q string
			switch fixture.TargetPool.Product {
			case types.ProductCockroachDB, types.ProductPostgreSQL:
				q = "SELECT count(*) FROM %s WHERE v = $1"
			case types.ProductOracle:
				q = "SELECT count(*) FROM %s WHERE v = :v"
			default:
				r.Fail("unimplemented product")
			}

			var count int
			r.NoError(pool.QueryRowContext(ctx, fmt.Sprintf(q, tgt), search).Scan(&count))
			log.Tracef("backfill count %d", count)
			if count == numEmits {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Ensure that deletes propagate correctly to t_1.
	_, err = pool.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE 1=1", tgts[0]))
	r.NoError(err)

	for {
		count, err := base.GetRowCount(ctx, fixture.TargetPool, tgts[0])
		r.NoError(err)
		if count == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify that t_2 was unchanged.
	count, err := base.GetRowCount(ctx, fixture.TargetPool, tgts[1])
	r.NoError(err)
	a.Equal(100, count)

}
