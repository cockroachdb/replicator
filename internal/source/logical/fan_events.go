// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// AwaitConsistentPoint implements State.  It delegates to the loop.
func (f *fanEvents) AwaitConsistentPoint(
	ctx context.Context, comparison AwaitComparison, point stamp.Stamp,
) (stamp.Stamp, error) {
	return f.loop.AwaitConsistentPoint(ctx, comparison, point)
}

// Backfill implements Events. It delegates to the enclosing loop.
func (f *fanEvents) Backfill(
	ctx context.Context, source string, backfiller Backfiller, options ...Option,
) error {
	return f.loop.doBackfill(ctx, source, backfiller, options...)
}

// GetConsistentPoint implements State. It delegates to the loop.
func (f *fanEvents) GetConsistentPoint() stamp.Stamp { return f.loop.GetConsistentPoint() }

// GetTargetDB implements State. It delegates to the loop.
func (f *fanEvents) GetTargetDB() ident.Ident { return f.loop.GetTargetDB() }

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

// stop implements Events and gracefully drains any pending work.
func (f *fanEvents) stop(ctx context.Context) error {
	if workers := f.workers; workers != nil {
		f.workers = nil
		return workers.Wait(ctx, true /* graceful */)
	}
	return nil
}
