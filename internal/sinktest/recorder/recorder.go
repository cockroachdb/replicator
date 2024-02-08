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

// Package recorder contains an acceptor implementation that records the
// method calls it sees.
package recorder

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
)

// Call is a union struct that retains the batch that was passed in.
type Call struct {
	Multi    *types.MultiBatch
	Table    *types.TableBatch
	Temporal *types.TemporalBatch
}

// Count returns the number of enclosed mutations.
func (c *Call) Count() int {
	if c.Multi != nil {
		return c.Multi.Count()
	}
	if c.Table != nil {
		return c.Table.Count()
	}
	if c.Temporal != nil {
		return c.Temporal.Count()
	}
	return 0
}

// Recorder implements [types.MultiAcceptor], recording its inputs.
// This type is safe for concurrent access.
type Recorder struct {
	Next types.MultiAcceptor
	mu   struct {
		sync.Mutex
		calls []*Call
	}
}

var _ types.MultiAcceptor = (*Recorder)(nil)

// AcceptTableBatch implements [types.TableAcceptor].
func (r *Recorder) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, options *types.AcceptOptions,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Pretend like we're a database.
	if err := ctx.Err(); err != nil {
		return err
	}
	err := r.Next.AcceptTableBatch(ctx, batch, options)
	if err == nil {
		r.mu.calls = append(r.mu.calls, &Call{Table: batch})
	}
	return err
}

// AcceptTemporalBatch implements [types.TemporalAcceptor].
func (r *Recorder) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, options *types.AcceptOptions,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Pretend like we're a database.
	if err := ctx.Err(); err != nil {
		return err
	}
	err := r.Next.AcceptTemporalBatch(ctx, batch, options)
	if err == nil {
		r.mu.calls = append(r.mu.calls, &Call{Temporal: batch})
	}
	return err
}

// AcceptMultiBatch implements [types.MultiAcceptor].
func (r *Recorder) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, options *types.AcceptOptions,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Pretend like we're a database.
	if err := ctx.Err(); err != nil {
		return err
	}
	err := r.Next.AcceptMultiBatch(ctx, batch, options)
	if err == nil {
		r.mu.calls = append(r.mu.calls, &Call{Multi: batch})
	}
	return nil
}

// Calls returns a copy of the recording.
func (r *Recorder) Calls() []*Call {
	r.mu.Lock()
	defer r.mu.Unlock()

	ret := make([]*Call, len(r.mu.calls))
	copy(ret, r.mu.calls)
	return ret
}

// Count returns the number of enclosed mutations.
func (r *Recorder) Count() int {
	var ret int
	for _, call := range r.Calls() {
		ret += call.Count()
	}
	return ret
}
