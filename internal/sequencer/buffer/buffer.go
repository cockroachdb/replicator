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

// Package buffer contains an in-memory implementation of
// [types.BatchReader] and a [sequencer.Shim] to install it in the
// stack.
package buffer

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
)

// Shim handles buffering and replay of in-memory transaction data.
//
// This shim should be used when writing push-type, single-stream
// frontends that can assemble entire transactions . The frontend must
// ensure that the timestamps on the provided batches are strictly
// monotonic, since this implementation will debounce duplicates based
// on a high-water mark.
//
// See [hlc.Clock].
type Shim struct {
	cfg *sequencer.Config
}

var _ sequencer.Shim = (*Shim)(nil)

// Wrap implements [sequencer.Shim].
func (b *Shim) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &buffer{b, delegate}, nil
}

type buffer struct {
	*Shim
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*buffer)(nil)

// Start implements [sequencer.Sequencer].
func (b *buffer) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	buf := New(b.cfg.TimestampLimit, opts.Bounds)

	opts = opts.Copy()
	opts.BatchReader = buf
	opts.Delegate = &cleaner{buf, opts.Delegate}

	_, stats, err := b.delegate.Start(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	buf.Retire(ctx, stats)
	return buf, stats, nil
}

type cleaner struct {
	buf      *Buffer
	delegate types.MultiAcceptor
}

var _ types.MultiAcceptor = (*cleaner)(nil)

func (c *cleaner) AcceptTableBatch(ctx context.Context, batch *types.TableBatch, options *types.AcceptOptions) error {
	if err := c.delegate.AcceptTableBatch(ctx, batch, options); err != nil {
		return err
	}
	_, _, err := c.buf.state.Update(func(s *state) (*state, error) {
		s.Mark(batch.Time)
		return s, nil
	})
	return err
}

func (c *cleaner) AcceptTemporalBatch(ctx context.Context, batch *types.TemporalBatch, options *types.AcceptOptions) error {
	if err := c.delegate.AcceptTemporalBatch(ctx, batch, options); err != nil {
		return err
	}
	_, _, err := c.buf.state.Update(func(s *state) (*state, error) {
		s.Mark(batch.Time)
		return s, nil
	})
	return err
}

func (c *cleaner) AcceptMultiBatch(ctx context.Context, batch *types.MultiBatch, options *types.AcceptOptions) error {
	if err := c.delegate.AcceptMultiBatch(ctx, batch, options); err != nil {
		return err
	}
	_, _, err := c.buf.state.Update(func(s *state) (*state, error) {
		for _, temp := range batch.Data {
			s.Mark(temp.Time)
		}
		return s, nil
	})
	return err
}
