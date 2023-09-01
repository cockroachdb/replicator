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

// serialEvents is a transaction-preserving implementation of Events.
type serialEvents struct {
	appliers   types.Appliers
	loop       *loop
	targetPool *types.TargetPool

	stamp stamp.Stamp    // the latest value passed to OnCommit.
	tx    types.TargetTx // db transaction created by OnCommit.
}

var _ Events = (*serialEvents)(nil)

// Backfill implements Events. It delegates to the enclosing loop.
func (e *serialEvents) Backfill(ctx context.Context, source string, backfiller Backfiller) error {
	return e.loop.doBackfill(ctx, source, backfiller)
}

// Flush returns nil, since OnData() writes values immediately.
func (e *serialEvents) Flush(context.Context) error {
	return nil
}

// GetConsistentPoint implements State. It delegates to the loop.
func (e *serialEvents) GetConsistentPoint() stamp.Stamp { return e.loop.GetConsistentPoint() }

// GetTargetDB implements State. It delegates to the loop.
func (e *serialEvents) GetTargetDB() ident.Schema { return e.loop.GetTargetDB() }

// NotifyConsistentPoint implements State.  It delegates to the loop.
func (e *serialEvents) NotifyConsistentPoint(
	ctx context.Context, comparison AwaitComparison, point stamp.Stamp,
) <-chan stamp.Stamp {
	return e.loop.NotifyConsistentPoint(ctx, comparison, point)
}

// OnBegin implements Events.
func (e *serialEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	if e.tx != nil {
		return errors.Errorf("OnBegin already called at %s", e.stamp)
	}
	e.stamp = point

	// Avoid storing TargetTx(nil) into struct field.
	tx, err := e.targetPool.BeginTx(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	e.tx = tx
	return nil
}

// OnCommit implements Events.
func (e *serialEvents) OnCommit(_ context.Context) error {
	if e.tx == nil {
		return errors.New("OnCommit called without matching OnBegin")
	}

	err := e.tx.Commit()
	e.tx = nil
	if err != nil {
		return errors.WithStack(err)
	}

	return e.loop.setConsistentPoint(e.stamp)
}

// OnData implements Events.
func (e *serialEvents) OnData(
	ctx context.Context, _ ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	app, err := e.appliers.Get(ctx, target)
	if err != nil {
		return err
	}
	return app.Apply(ctx, e.tx, muts)
}

// OnRollback implements Events and delegates to drain.
func (e *serialEvents) OnRollback(ctx context.Context, msg Message) error {
	if !IsRollback(msg) {
		return errors.New("the rollback message must be passed to OnRollback")
	}
	return e.drain(ctx)
}

// Stopping implements State and delegates to the enclosing loop.
func (e *serialEvents) Stopping() <-chan struct{} {
	return e.loop.Stopping()
}

// drain implements Events.
func (e *serialEvents) drain(_ context.Context) error {
	if e.tx != nil {
		_ = e.tx.Rollback()
	}
	e.stamp = nil
	e.tx = nil
	return nil
}
