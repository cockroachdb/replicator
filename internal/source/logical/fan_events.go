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

package logical

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

// fanEvents is a high-throughput implementation of Events which
// does not preserve transaction boundaries.
type fanEvents struct {
	loop *loop // The underlying loop.

	workers *fanWorkers // Contains the stamp passed into OnBegin().
}

var _ Events = (*fanEvents)(nil)

// Backfill implements Events. It delegates to the enclosing loop.
func (f *fanEvents) Backfill(
	ctx context.Context, source string, backfiller Backfiller, options ...Option,
) error {
	return f.loop.doBackfill(ctx, source, backfiller, options...)
}

// Flush implements Events.
func (f *fanEvents) Flush(ctx context.Context) error {
	workers := f.workers
	if workers == nil {
		return errors.New("Flush() called without OnBegin()")
	}
	return workers.Flush(ctx)
}

// GetConsistentPoint implements State. It delegates to the loop.
func (f *fanEvents) GetConsistentPoint() stamp.Stamp { return f.loop.GetConsistentPoint() }

// GetTargetDB implements State. It delegates to the loop.
func (f *fanEvents) GetTargetDB() ident.Schema { return f.loop.GetTargetDB() }

// NotifyConsistentPoint implements State.  It delegates to the loop.
func (f *fanEvents) NotifyConsistentPoint(
	ctx context.Context, comparison AwaitComparison, point stamp.Stamp,
) <-chan stamp.Stamp {
	return f.loop.NotifyConsistentPoint(ctx, comparison, point)
}

// OnBegin implements Events.
func (f *fanEvents) OnBegin(ctx context.Context, s stamp.Stamp) error {
	if f.workers != nil {
		return errors.New("OnBegin() called without OnCommit() or reset()")
	}
	f.workers = newFanWorkers(ctx, f.loop, s)
	return nil
}

// OnCommit implements Events.
func (f *fanEvents) OnCommit(ctx context.Context) error {
	workers := f.workers
	if workers == nil {
		return errors.New("OnCommit() called without OnBegin()")
	}
	err := workers.Wait(ctx, true)
	f.workers = nil
	if err != nil {
		return err
	}
	return f.loop.setConsistentPoint(workers.Stamp())
}

// OnData implements Events and delegates to the enclosed fan.
func (f *fanEvents) OnData(
	_ context.Context, _ ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	if f.workers == nil {
		return errors.New("OnData() called without OnBegin")
	}
	return f.workers.Enqueue(target, muts)
}

// OnRollback implements Events and resets any pending work.
func (f *fanEvents) OnRollback(ctx context.Context, msg Message) error {
	if !IsRollback(msg) {
		return errors.New("the rollback message must be passed to OnRollback")
	}
	if workers := f.workers; workers != nil {
		f.workers = nil
		return workers.Wait(ctx, false /* abandon */)
	}
	return nil
}

// Stopping implements State and delegates to the enclosing loop.
func (f *fanEvents) Stopping() <-chan struct{} {
	return f.loop.Stopping()
}

// drain implements Events and gracefully drains any pending work.
func (f *fanEvents) drain(ctx context.Context) error {
	if workers := f.workers; workers != nil {
		f.workers = nil
		return workers.Wait(ctx, true /* graceful */)
	}
	return nil
}
