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

// Package chaos allows errors to be introduced in a sequencer stack.
package chaos

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
)

// ErrChaos can be used in tests to differentiate intentionally injected
// errors versus unexpected errors.
var ErrChaos = errors.New("chaos")

// Chaos is a [sequencer.Shim] that randomly injects errors into the
// acceptor base on [sequencer.StartOptions.Chaos].
type Chaos struct {
	Config *sequencer.Config
}

var _ sequencer.Shim = (*Chaos)(nil)

// Wrap implements [sequencer.Shim].
func (c *Chaos) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &chaos{delegate: delegate, prob: c.Config.Chaos}, nil
}

type chaos struct {
	delegate sequencer.Sequencer
	prob     float32
}

var _ sequencer.Sequencer = (*chaos)(nil)

// Start implements [sequencer.Sequencer].
func (c *chaos) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	if c.prob <= 0 {
		return c.delegate.Start(ctx, opts)
	}
	// Inject errors deeper into the stack.
	opts = opts.Copy()
	opts.Delegate = &acceptor{delegate: opts.Delegate, prob: c.prob}
	acc, stat, err := c.delegate.Start(ctx, opts)
	// Inject errors at periphery of stack.
	return &acceptor{acc, c.prob}, stat, err
}

// Unwrap is an informal protocol to access the delegate.
func (c *chaos) Unwrap() sequencer.Sequencer {
	return c.delegate
}

// acceptor will inject [ErrChaos] with the configured probability. If
// the method call should result in a chaos error, the error may be
// returned after calling the delegate method.
type acceptor struct {
	delegate types.MultiAcceptor
	prob     float32
}

var _ types.MultiAcceptor = (*acceptor)(nil)

// AcceptTableBatch implements [types.MultiAcceptor].
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if chaos := a.chaos(); chaos != nil {
		if rand.Intn(2) == 0 {
			return chaos
		}
		if err := a.delegate.AcceptTableBatch(ctx, batch, opts); err != nil {
			return err
		}
		a.delay()
		return chaos
	}
	return a.delegate.AcceptTableBatch(ctx, batch, opts)
}

// AcceptTemporalBatch implements [types.MultiAcceptor].
func (a *acceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	if chaos := a.chaos(); chaos != nil {
		if rand.Intn(2) == 0 {
			return chaos
		}
		if err := a.delegate.AcceptTemporalBatch(ctx, batch, opts); err != nil {
			return err
		}
		a.delay()
		return chaos
	}
	return a.delegate.AcceptTemporalBatch(ctx, batch, opts)
}

// AcceptMultiBatch implements [types.MultiAcceptor].
func (a *acceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	if chaos := a.chaos(); chaos != nil {
		if rand.Intn(2) == 0 {
			return chaos
		}
		if err := a.delegate.AcceptMultiBatch(ctx, batch, opts); err != nil {
			return err
		}
		a.delay()
		return chaos
	}
	return a.delegate.AcceptMultiBatch(ctx, batch, opts)
}

// Unwrap is an informal protocol to return the delegate.
func (a *acceptor) Unwrap() types.MultiAcceptor {
	return a.delegate
}

// chaos exists to have an easy place to set a breakpoint.
func (a *acceptor) chaos() error {
	if rand.Float32() <= a.prob {
		return ErrChaos
	}
	return nil
}

// delay exists to add jitter within the system. It can help shake loose
// race conditions.
func (a *acceptor) delay() {
	time.Sleep(time.Duration(rand.Int63n(time.Millisecond.Nanoseconds())))
}
