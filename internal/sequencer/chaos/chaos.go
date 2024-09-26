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
) (*notify.Var[sequencer.Stat], error) {
	if c.count == 0 {
		return c.delegate.Start(ctx, opts)
	}
	t := &tracker{count: c.count}
	// Inject errors on the input and output side.
	opts = opts.Copy()
	opts.BatchReader = &reader{delegate: opts.BatchReader, tracker: t}
	opts.Delegate = &acceptor{delegate: opts.Delegate, tracker: t}
	stat, err := c.delegate.Start(ctx, opts)
	return stat, err
}

// Unwrap is an informal protocol to access the delegate.
func (c *chaos) Unwrap() sequencer.Sequencer {
	return c.delegate
}

// acceptor will inject [ErrChaos] count times per distinct call stack.
// If the method call should result in a chaos error, the error may be
// returned after calling the delegate method.
type acceptor struct {
	delegate types.MultiAcceptor
	tracker  *tracker
}

var _ types.MultiAcceptor = (*acceptor)(nil)

// AcceptTableBatch implements [types.MultiAcceptor].
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if chaos := a.tracker.chaos(); chaos != nil {
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
	if chaos := a.tracker.chaos(); chaos != nil {
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
	if chaos := a.tracker.chaos(); chaos != nil {
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

// delay exists to add jitter within the system. It can help shake loose
// race conditions.
func (a *acceptor) delay() {
	time.Sleep(time.Duration(rand.Int63n(time.Millisecond.Nanoseconds())))
}

// reader injects [ErrChaos] into the [types.BatchReader] stream.
type reader struct {
	delegate types.BatchReader
	tracker  *tracker
}

var _ types.BatchReader = (*reader)(nil)

func (r *reader) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	// Simulate startup errors.
	if err := r.tracker.chaos(); err != nil {
		return nil, err
	}

	// Open channel from delegate.
	src, err := r.delegate.Read(ctx)
	if err != nil {
		return nil, err
	}

	// Copy cursors downstream, injecting errors.
	out := make(chan *types.BatchCursor, 2)
	ctx.Go(func(ctx *stopper.Context) error {
		defer close(out)
		for {
			select {
			case cur, open := <-src:
				if !open {
					return nil
				}
				switch {
				case cur.Error != nil:
					// Let underlying error percolate.
				case cur.Batch == nil:
					// Progress-only update
					if err := r.tracker.chaos(); err != nil {
						cur.Error = err
					}
				default:
					// Data update.
					if err := r.tracker.chaos(); err != nil {
						cur.Error = err
					}
				}
				select {
				case out <- cur:
				case <-ctx.Stopping():
					return nil
				}

			case <-ctx.Stopping():
				return nil
			}
		}
	})

	return out, nil
}

// tracker limits the number of times chaos will be injected.
type tracker struct {
	count int
	mu    struct {
		sync.Mutex
		seen map[[maxFrames]uintptr]int
	}
}

func (a *tracker) chaos() error {
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
