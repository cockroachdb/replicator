// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type fakeMessage int

var _ stamp.Stamp = fakeMessage(0)

func (f fakeMessage) Less(other stamp.Stamp) bool {
	return f < other.(fakeMessage)
}
func (f fakeMessage) String() string {
	return strconv.FormatInt(int64(f), 10)
}

// generatorDialect implements logical.Dialect to generate mutations
// for a KV table.
type generatorDialect struct {
	// Send an update for each table.
	tables []ident.Table

	// Counters to ensure we shut down cleanly.
	atomic struct {
		readIntoExits int32
		processExits  int32
	}

	workRequested chan struct{}
	readIntoMu    struct {
		sync.Mutex
		totalRequested int
		lastBatchSent  int
	}

	processMu struct {
		sync.Mutex
		messages []Message
	}
}

var _ Dialect = (*generatorDialect)(nil)

func newGenerator(tables []ident.Table) *generatorDialect {
	return &generatorDialect{
		tables:        tables,
		workRequested: make(chan struct{}, 1),
	}
}

func (g *generatorDialect) emit(numBatches int) {
	g.readIntoMu.Lock()
	defer g.readIntoMu.Unlock()
	g.readIntoMu.totalRequested += numBatches
	select {
	case g.workRequested <- struct{}{}:
	default:
		panic("work request channel is full")
	}
}

// ReadInto waits to be woken up by a call to emit, then writes
// n-many counter messages into the channel.
func (g *generatorDialect) ReadInto(ctx context.Context, ch chan<- Message, state State) error {
	log.Trace("ReadInto starting")
	defer atomic.AddInt32(&g.atomic.readIntoExits, 1)

	nextBatchNumber := 0
	// If we're recovering from a failure condition, reset to a consistent point.
	if prev := state.GetConsistentPoint(); prev != nil {
		nextBatchNumber = int(prev.(fakeMessage)) + 1
		log.Tracef("restarting at %d", nextBatchNumber)
	}

	for {
		g.readIntoMu.Lock()
		requested := g.readIntoMu.totalRequested
		g.readIntoMu.Unlock()

		// emit requested number of messages.
		for nextBatchNumber < requested {
			log.Tracef("sending %d", nextBatchNumber)
			// Non-blocking send if the ctx is shut down.
			select {
			case ch <- fakeMessage(nextBatchNumber):
				g.readIntoMu.Lock()
				g.readIntoMu.lastBatchSent = nextBatchNumber
				g.readIntoMu.Unlock()

			case <-ctx.Done():
				return ctx.Err()
			}

			nextBatchNumber++
		}

		// Wait for more work or to be shut down
		select {
		case <-g.workRequested:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Each message triggers a transaction flow to send one mutation to each
//  configured table.
func (g *generatorDialect) Process(ctx context.Context, ch <-chan Message, events Events) error {
	log.Trace("Process starting")
	defer atomic.AddInt32(&g.atomic.processExits, 1)

	for {
		// Non-blocking read if the context is shut down.
		var msg fakeMessage
		select {
		case m := <-ch:
			g.processMu.Lock()
			g.processMu.messages = append(g.processMu.messages, m)
			g.processMu.Unlock()

			// Ensure that rollbacks result in proper resynchronization.
			if IsRollback(m) {
				if err := events.OnRollback(ctx, m); err != nil {
					return err
				}
				continue
			}
			msg = m.(fakeMessage)
			log.Tracef("received %d", msg)
		case <-ctx.Done():
			return ctx.Err()
		}

		if err := events.OnBegin(ctx, msg); err != nil {
			return err
		}

		for _, tbl := range g.tables {
			mut := types.Mutation{
				Key:  []byte(fmt.Sprintf(`[%d]`, msg)),
				Data: []byte(fmt.Sprintf(`{"k":%d,"v":"%d"}`, msg, msg)),
			}
			if err := events.OnData(ctx, tbl, []types.Mutation{mut}); err != nil {
				return err
			}
		}

		if err := events.OnCommit(ctx); err != nil {
			return err
		}
	}
}

func (g *generatorDialect) UnmarshalStamp(stamp string) (stamp.Stamp, error) {
	res, err := strconv.ParseInt(stamp, 0, 64)
	return fakeMessage(res), err
}

func TestLogical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testLogicalSmoke(t, false, false) })
	t.Run("consistent-chaos", func(t *testing.T) { testLogicalSmoke(t, false, true) })
	t.Run("immediate", func(t *testing.T) { testLogicalSmoke(t, true, false) })
	t.Run("immediate-chaos", func(t *testing.T) { testLogicalSmoke(t, true, true) })
}

func testLogicalSmoke(t *testing.T, immediate, withChaos bool) {
	a := assert.New(t)

	ctx, info, cancel := sinktest.Context()
	defer cancel()
	pool := info.Pool()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

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

	var dialect Dialect = gen
	if withChaos {
		dialect = WithChaos(gen, 0.01)
	}

	cfg := &Config{
		ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		Immediate:    immediate,
		RetryDelay:   time.Nanosecond,
		TargetConn:   pool.Config().ConnString(),
		TargetDB:     dbName,
	}
	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()
	stopped, err := Start(loopCtx, cfg, dialect)
	if !a.NoError(err) {
		return
	}

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
	case <-stopped:
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
