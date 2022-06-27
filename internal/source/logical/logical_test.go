// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLogical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testLogicalSmoke(t, false, false) })
	t.Run("consistent-chaos", func(t *testing.T) { testLogicalSmoke(t, false, true) })
	t.Run("immediate", func(t *testing.T) { testLogicalSmoke(t, true, false) })
	t.Run("immediate-chaos", func(t *testing.T) { testLogicalSmoke(t, true, true) })
}

func testLogicalSmoke(t *testing.T, immediate, withChaos bool) {
	a := assert.New(t)

	// Create a basic test fixture.
	fixture, cancel, err := sinktest.NewBaseFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TestDB.Ident()
	pool := fixture.Pool

	// Create some tables.
	tgts := []ident.Table{
		ident.NewTable(dbName, ident.Public, ident.New("t1")),
		ident.NewTable(dbName, ident.Public, ident.New("t2")),
		ident.NewTable(dbName, ident.Public, ident.New("t3")),
		ident.NewTable(dbName, ident.Public, ident.New("t4")),
	}

	for _, tgt := range tgts {
		var schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)`, tgt)
		if _, err := pool.Exec(ctx, schema); !a.NoError(err) {
			return
		}
	}

	gen := newGenerator(tgts)
	const numEmits = 100
	gen.emit(numEmits)

	var dialect logical.Dialect = gen
	if withChaos {
		dialect = logical.WithChaos(gen, 0.01)
	}

	cfg := &logical.Config{
		ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		Immediate:    immediate,
		RetryDelay:   time.Nanosecond,
		StagingDB:    fixture.StagingDB.Ident(),
		TargetConn:   pool.Config().ConnString(),
		TargetDB:     dbName,
	}

	loop, cancelLoop, err := logical.Start(ctx, cfg, dialect)
	if !a.NoError(err) {
		return
	}
	defer cancelLoop()

	// Wait for replication.
	for _, tgt := range tgts {
		for {
			var count int
			if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tgt)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Trace("backfill count", count)
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
	if !withChaos {
		a.Equal(int32(1), atomic.LoadInt32(&gen.atomic.processExits))
		a.Equal(int32(1), atomic.LoadInt32(&gen.atomic.readIntoExits))
	}

	// Verify that we did drain the generator.
	gen.readIntoMu.Lock()
	defer gen.readIntoMu.Unlock()
	a.Equal(numEmits-1, gen.readIntoMu.lastBatchSent)

	// Verify that we saw all messages.
	gen.processMu.Lock()
	defer gen.processMu.Unlock()
	// Verify that we saw each unique key at least once. The actual
	// slice of messages will contain repeated entries in the chaos
	// tests.
	found := make(map[fakeMessage]struct{})
	for _, msg := range gen.processMu.messages {
		if fake, ok := msg.(fakeMessage); ok {
			found[fake] = struct{}{}
		}
	}
	a.Len(found, numEmits)
}
