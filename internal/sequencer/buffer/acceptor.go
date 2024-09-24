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
	"container/ring"
	"context"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
)

// Buffer is a [types.MultiAcceptor] and [types.BatchReader] that stores
// transaction data in a bounded ring buffer.
type Buffer struct {
	bounds *notify.Var[hlc.Range]
	state  *notify.Var[*state]
}

var (
	_ types.BatchReader   = (*Buffer)(nil)
	_ types.MultiAcceptor = (*Buffer)(nil)
)

// New constructs a [Buffer] with the given capacity.
func New(cap int, bounds *notify.Var[hlc.Range]) *Buffer {
	r := ring.New(cap)
	return &Buffer{
		bounds: bounds,
		state: notify.VarOf(&state{
			marked: make(map[hlc.Time]struct{}, cap),
			head:   r,
			tail:   r,
		}),
	}
}

// AcceptMultiBatch implements [types.MultiAcceptor]. This method will
// block if the ring buffer is full.
func (a *Buffer) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, _ *types.AcceptOptions,
) error {
	return a.append(ctx, batch.Data)
}

// AcceptTableBatch implements [types.MultiAcceptor]. This method will
// block if the ring buffer is full.
func (a *Buffer) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, _ *types.AcceptOptions,
) error {
	temp := &types.TemporalBatch{Time: batch.Time}
	temp.Data.Put(batch.Table, batch)
	return a.append(ctx, []*types.TemporalBatch{temp})
}

// AcceptTemporalBatch implements [types.MultiAcceptor]. This method
// will block if the ring buffer is full.
func (a *Buffer) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, _ *types.AcceptOptions,
) error {
	return a.append(ctx, []*types.TemporalBatch{batch})
}

// Read implements [types.BatchReader]. The returned channel will emit any data in the buffer
func (a *Buffer) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	// Small buffer since traversing pointers is faster than a db.
	ch := make(chan *types.BatchCursor, 2)

	ctx.Go(func(ctx *stopper.Context) error {
		defer close(ch)
		bounds, boundsChanged := a.bounds.Get()
		var lastBounds hlc.Range
		var marker *readMarker
		for {
			// The ring buffer elements are internally mutable, so we
			// need a locked read on state.
			var toSend []*types.TemporalBatch
			stateChanged, _ := a.state.Peek(func(s *state) error {
				// Make state iterable:
				// https://github.com/cockroachdb/replicator/issues/1015
				marker, toSend = s.Read(marker, nil)
				return nil
			})

			// Send the batches upstream.
			for _, batch := range toSend {
				cur := &types.BatchCursor{
					Batch:    batch,
					Progress: hlc.RangeIncluding(hlc.Zero(), batch.Time),
				}

				select {
				case ch <- cur:
				case <-ctx.Stopping():
					return nil
				}
			}

			// If the buffer is empty and the resolving bounds have
			// advanced, send a progress-only notification. This update
			// will be reflected in the delegate sequencer's stats.
			if len(toSend) == 0 && hlc.Compare(bounds.Max(), lastBounds.Max()) > 0 {
				lastBounds = bounds
				cur := &types.BatchCursor{
					Progress: bounds,
				}

				select {
				case ch <- cur:
				case <-ctx.Stopping():
					return nil
				}
			}

			// Wait for something to change.
			select {
			case <-boundsChanged:
				bounds, boundsChanged = a.bounds.Get()
			case <-stateChanged:
			case <-ctx.Stopping():
				return nil
			}
		}
	})

	return ch, nil
}

// Retire starts a background process to retire old mutations from the
// buffer. It will be triggered when the underlying sequencer reports
// forward progress.
func (a *Buffer) Retire(ctx *stopper.Context, stats *notify.Var[sequencer.Stat]) {
	// Retire old data as the delegate makes partial progress.
	ctx.Go(func(ctx *stopper.Context) error {
		// Ignoring error since callback returns nil.
		_, _ = stopvar.DoWhenChanged(ctx, nil, stats, func(ctx *stopper.Context, _, stat sequencer.Stat) error {
			_, _, _ = a.state.Update(func(s *state) (*state, error) {
				if !s.Drain() {
					return nil, notify.ErrNoUpdate
				}
				return s, nil
			})
			return nil
		})
		return nil
	})
}

// append the batches to the ring buffer. This method will block if the
// ring is full
func (a *Buffer) append(ctx context.Context, batches []*types.TemporalBatch) error {
	if len(batches) == 0 {
		return nil
	}

	batchIdx := 0
	stop := stopper.From(ctx)

	// This loop will iterate multiple times if the ring is full.
	for {
		// Append as many entries to the ring as we can.
		_, stateChanged, _ := a.state.Update(func(s *state) (*state, error) {
			// Already full, so do nothing.
			if s.Full() {
				return nil, notify.ErrNoUpdate
			}

			for _, batch := range batches[batchIdx:] {
				batchIdx++
				s.Append(batch)
				// No more space available in the buffer.
				if s.Full() {
					break
				}
			}
			return s, nil
		})

		// All entries appended.
		if batchIdx == len(batches) {
			return nil
		}

		// Wait for the ring to drain.
		select {
		case <-stateChanged:
		case <-stop.Stopping():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
