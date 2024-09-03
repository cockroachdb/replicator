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
	"runtime"
	"sync"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
)

// The number of stack frames to consider when determining unique call
// sites. This was determined empirically.
const maxFrames = 25

// ErrChaos can be used in tests to differentiate intentionally injected
// errors versus unexpected errors.
var ErrChaos = errors.New("chaos")

// Chaos is a [sequencer.Shim] that randomly injects errors into the
// acceptor based on [sequencer.StartOptions.Chaos].
type Chaos struct {
	Config *sequencer.Config
}

var _ sequencer.Shim = (*Chaos)(nil)

// Wrap implements [sequencer.Shim].
func (c *Chaos) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &chaos{count: c.Config.Chaos, delegate: delegate}, nil
}

type chaos struct {
	count    int
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*chaos)(nil)

// Start implements [sequencer.Sequencer].
func (c *chaos) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	if c.count == 0 {
		return c.delegate.Start(ctx, opts)
	}
	// Inject errors deeper into the stack.
	opts = opts.Copy()
	opts.Delegate = &acceptor{count: c.count, delegate: opts.Delegate}
	acc, stat, err := c.delegate.Start(ctx, opts)
	// Inject errors at periphery of stack.
	return &acceptor{count: c.count, delegate: acc}, stat, err
}

// Unwrap is an informal protocol to access the delegate.
func (c *chaos) Unwrap() sequencer.Sequencer {
	return c.delegate
}

// acceptor will inject [ErrChaos] count times per distinct call stack.
// If the method call should result in a chaos error, the error may be
// returned after calling the delegate method.
type acceptor struct {
	count    int
	delegate types.MultiAcceptor
	mu       struct {
		sync.Mutex
		seen map[[maxFrames]uintptr]int
	}
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
	// We ignore Callers(), chaos(), and acceptor method.
	var stack [maxFrames]uintptr
	runtime.Callers(3, stack[:])

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.mu.seen == nil {
		a.mu.seen = make(map[[maxFrames]uintptr]int)
	}

	count := a.mu.seen[stack]
	if count >= a.count {
		return nil
	}
	a.mu.seen[stack] = count + 1
	return ErrChaos
}

// delay exists to add jitter within the system. It can help shake loose
// race conditions.
func (a *acceptor) delay() {
	time.Sleep(time.Duration(rand.Int63n(time.Millisecond.Nanoseconds())))
}
