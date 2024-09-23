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

package buffer

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
)

// node is a trivial linked-list element.
type node struct {
	batch *types.TemporalBatch
	next  *node
}

// state maintains a linked-list buffer of mutations to replay in order
// to satisfy the [types.BatchReader.Read] contract.
type state struct {
	head *node
	tail *node

	// Acts as a ratchet based on highest timestamp in the buffer.
	highWater hlc.Time
}

// copy returns a shallow copy of the state.
func (s *state) copy() *state {
	cpy := *s
	return &cpy
}

type acceptor struct {
	bounds *notify.Var[hlc.Range]
	state  *notify.Var[*state]
}

var (
	_ types.BatchReader   = (*acceptor)(nil)
	_ types.MultiAcceptor = (*acceptor)(nil)
)

func (a *acceptor) AcceptMultiBatch(
	_ context.Context, batch *types.MultiBatch, _ *types.AcceptOptions,
) error {
	return a.append(batch.Data)
}

func (a *acceptor) AcceptTableBatch(
	_ context.Context, batch *types.TableBatch, _ *types.AcceptOptions,
) error {
	temp := &types.TemporalBatch{Time: batch.Time}
	temp.Data.Put(batch.Table, batch)
	return a.append([]*types.TemporalBatch{temp})
}

func (a *acceptor) AcceptTemporalBatch(
	_ context.Context, batch *types.TemporalBatch, _ *types.AcceptOptions,
) error {
	return a.append([]*types.TemporalBatch{batch})
}

// Read implements [types.BatchReader]. It operates, unlocked, on state
// snapshots. When new data arrives, the state will be replaced and
// have a new tail value.
func (a *acceptor) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	ch := make(chan *types.BatchCursor, 1024)

	ctx.Go(func(ctx *stopper.Context) error {
		defer close(ch)

		// Handle the initial case where we're replaying the buffer.
		s, stateChanged := a.state.Get()
		var last *node

		for {
			// This will happen whenever we're resuming after emptying
			// the buffer.
			if last == nil {
				last = s.head
			}
			// Do work if there's something to process.
			for last != nil {
				cur := &types.BatchCursor{
					Batch:    last.batch,
					Progress: hlc.RangeIncluding(hlc.Zero(), last.batch.Time),
				}

				select {
				case ch <- cur:
				case <-ctx.Stopping():
					return nil
				}

				// Break when we reach the last element. We can't rely
				// on looking at s.tail.next, since we don't hold a lock
				// on state, and the next field is mutable.
				if last == s.tail {
					break
				}
				last = last.next
			}

			select {
			case <-stateChanged:
				s, stateChanged = a.state.Get()
			case <-ctx.Stopping():
				return nil
			}
		}
	})

	return ch, nil
}

func (a *acceptor) append(batches []*types.TemporalBatch) error {
	if len(batches) == 0 {
		return nil
	}

	// Ignoring error since inner callback always succeeds.
	_, _, _ = a.state.Update(func(s *state) (*state, error) {
		didChange := false
		for _, batch := range batches {
			// If the frontend redelivers data we've already seen (e.g.
			// network glitch forces reconnection to the source), we can
			// ignore it. This requires that the source has a strictly
			// monotonic clock sequence.
			if hlc.Compare(batch.Time, s.highWater) <= 0 {
				continue
			}

			// If we have a change, make a copy of the state to avoid
			// mutating a visible object.
			if !didChange {
				didChange = true
				s = s.copy()
			}
			s.highWater = batch.Time

			n := &node{batch: batch}
			if s.tail == nil {
				s.head = n
				s.tail = n
			} else {
				s.tail.next = n
				s.tail = n
			}
		}
		if !didChange {
			return nil, notify.ErrNoUpdate
		}
		return s, nil
	})
	return nil
}

// cleaner implements a background process to retire old mutations from
// the buffer. It will be triggered when the underlying sequencer
// reports forward progress.
func (a *acceptor) cleaner(ctx *stopper.Context, statVar *notify.Var[sequencer.Stat]) {
	// Ignoring error since callback returns nil.
	_, _ = stopvar.DoWhenChanged(ctx, nil, statVar, func(ctx *stopper.Context, _, stat sequencer.Stat) error {
		progress := sequencer.CommonProgress(stat).Max()
		// Ignoring error since callback returns nil.
		_, _, _ = a.state.Update(func(s *state) (*state, error) {
			didUpdate := false
			for s.head != nil {
				// Retain values after the progress mark.
				if hlc.Compare(s.head.batch.Time, progress) > 0 {
					break
				}

				// Create a copy of the state before mutating.
				if !didUpdate {
					s = s.copy()
					didUpdate = true
				}

				// We have a lock on state, so it's safe to reference
				// the next field here.
				s.head = s.head.next

				// Empty case, fast-forward the stat to the last
				// received bounds.
				if s.head == nil {
					s.tail = nil
					break
				}
			}
			if !didUpdate {
				return nil, notify.ErrNoUpdate
			}
			return s, nil
		})
		return nil
	})
}

func (a *acceptor) fastForward(ctx *stopper.Context, statVar *notify.Var[sequencer.Stat]) {
	bounds, boundsChanged := a.bounds.Get()
	s, stateChanged := a.state.Get()

	for {
		if s.head == nil {
			_, _, _ = statVar.Update(func(stat sequencer.Stat) (sequencer.Stat, error) {
				progress := sequencer.CommonProgress(stat)
				if hlc.Compare(bounds.Max(), progress.Max()) <= 0 {
					return nil, notify.ErrNoUpdate
				}
				progress = hlc.RangeIncluding(bounds.Max(), bounds.Max())
				stat = stat.Copy()
				for table := range stat.Progress().Keys() {
					stat.Progress().Put(table, progress)
				}
				return stat, nil
			})
		}

		select {
		case <-boundsChanged:
			bounds, boundsChanged = a.bounds.Get()
		case <-stateChanged:
			s, stateChanged = a.state.Get()
		case <-ctx.Stopping():
			return
		}
	}
}
