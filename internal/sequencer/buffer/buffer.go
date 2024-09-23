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

// Package buffer contains a [sequencer.Shim] that present its data to
// the underlying sequencer as a stream of [types.BatchCursor] updates.
package buffer

import (
	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
)

// Buffer is a [sequencer.Shim] whose acceptor will present its data to
// the underlying sequencer as a stream of [types.BatchCursor] updates.
// This shim will handle buffering and replay of transaction data to
// accommodate transient errors when writing to the target database.
//
// This shim should be used when writing frontends that wish to take
// advantage of the core sequencer and which can assemble entire
// transactions (e.g. single-stream replication protocols). The frontend
// must ensure that the timestamps on the provided batches are strictly
// monotonic, since this implementation will debounce duplicates based
// on a high-water mark.
type Buffer struct {
	cfg *sequencer.Config
}

var _ sequencer.Shim = (*Buffer)(nil)

// Wrap implements [sequencer.Shim].
func (b *Buffer) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &buffer{b, delegate}, nil
}

type buffer struct {
	*Buffer
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*buffer)(nil)

// Start implements [sequencer.Sequencer].
func (b *buffer) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	acc := &acceptor{
		bounds: opts.Bounds,
		state:  notify.VarOf(&state{}),
	}
	opts = opts.Copy()
	opts.BatchReader = acc

	_, stats, err := b.delegate.Start(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	// Retire old data as the delegate makes partial progress.
	ctx.Go(func(ctx *stopper.Context) error {
		acc.cleaner(ctx, stats)
		return nil
	})
	// Fast-forward progress when there is no buffered data.
	ctx.Go(func(ctx *stopper.Context) error {
		acc.fastForward(ctx, stats)
		return nil
	})
	return acc, stats, nil
}
