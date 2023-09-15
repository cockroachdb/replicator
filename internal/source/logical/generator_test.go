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

// This file contains support code for logical_test.go.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	log "github.com/sirupsen/logrus"
)

type fakeMessage struct {
	Index int       `json:"idx"`
	TS    time.Time `json:"ts"`
}

var _ stamp.Stamp = (*fakeMessage)(nil)

func (f *fakeMessage) AsInt() int        { return f.Index }
func (f *fakeMessage) AsTime() time.Time { return f.TS }

func (f *fakeMessage) Less(other stamp.Stamp) bool {
	return f.AsInt() < other.(*fakeMessage).AsInt()
}

// generatorDialect implements logical.Dialect to generate mutations
// for a KV table.
type generatorDialect struct {
	// Send an update for each table.
	tables []ident.Table

	// Counters to ensure we shut down cleanly.
	atomic struct {
		readIntoExits atomic.Int32
		processExits  atomic.Int32
	}

	workRequested chan struct{}
	readIntoMu    struct {
		sync.Mutex
		totalRequested int
		lastBatchSent  int
	}

	processMu struct {
		sync.Mutex
		messages []logical.Message
	}
}

var (
	_ logical.Backfiller = (*generatorDialect)(nil)
	_ logical.Dialect    = (*generatorDialect)(nil)
)

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

// BackfillInto delegates to ReadInto.
func (g *generatorDialect) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	return g.ReadInto(ctx, ch, state)
}

// ReadInto waits to be woken up by a call to emit, then writes
// n-many counter messages into the channel.
func (g *generatorDialect) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	log.Trace("ReadInto starting")
	defer g.atomic.readIntoExits.Add(1)

	var nextBatchNumber int
	// If we're recovering from a failure condition, reset to a consistent point.
	prev, _ := state.GetConsistentPoint()
	if prev != nil {
		nextBatchNumber = prev.(*fakeMessage).AsInt() + 1
		log.Tracef("restarting at %d", nextBatchNumber)
	}

	for {
		g.readIntoMu.Lock()
		requested := g.readIntoMu.totalRequested
		g.readIntoMu.Unlock()

		// emit requested number of messages.
		for nextBatchNumber <= requested {
			log.Tracef("sending %d", nextBatchNumber)
			// Non-blocking send if the ctx is shut down.
			select {
			case ch <- &fakeMessage{nextBatchNumber, time.Now()}:
				g.readIntoMu.Lock()
				g.readIntoMu.lastBatchSent = nextBatchNumber
				g.readIntoMu.Unlock()

			case <-state.Stopping():
				// Graceful shutdown requested.
				return nil

			case <-ctx.Done():
				return ctx.Err()
			}

			nextBatchNumber++
		}

		// Wait for more work or to be shut down
		select {
		case <-g.workRequested:
		case <-state.Stopping():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Process triggers a transaction flow to send one mutation to each
// configured table.
func (g *generatorDialect) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	log.Trace("Process starting")
	defer g.atomic.processExits.Add(1)

	var batch logical.Batch
	for m := range ch {
		g.processMu.Lock()
		g.processMu.messages = append(g.processMu.messages, m)
		g.processMu.Unlock()

		// Ensure that rollbacks result in proper resynchronization.
		if logical.IsRollback(m) {
			if batch != nil {
				if err := batch.OnRollback(ctx); err != nil {
					return err
				}
			}
			batch = nil
			continue
		}
		msg := m.(*fakeMessage)
		log.Tracef("received %d", msg.Index)

		var err error
		batch, err = events.OnBegin(ctx)
		if err != nil {
			return err
		}

		for _, tbl := range g.tables {
			mut := types.Mutation{
				Key:  []byte(fmt.Sprintf(`[%d]`, msg.Index)),
				Data: []byte(fmt.Sprintf(`{"k":%[1]d,"v":"%[1]d","ref": %[1]d}`, msg.Index)),
			}
			if err := batch.OnData(ctx, tbl.Table(), tbl, []types.Mutation{mut}); err != nil {
				return err
			}
		}

		// Ensure that we can wait for data in flight, if necessary.
		if err := batch.Flush(ctx); err != nil {
			return err
		}

		select {
		case err := <-batch.OnCommit(ctx):
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		// Record success.
		if err := events.SetConsistentPoint(ctx, msg); err != nil {
			return err
		}

		batch = nil
	}
	return nil
}

func (g *generatorDialect) ZeroStamp() stamp.Stamp {
	return &fakeMessage{}
}
