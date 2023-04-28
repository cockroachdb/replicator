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
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// serialEvents is a transaction-preserving implementation of Events.
type serialEvents struct {
	appliers types.Appliers
	loop     *loop
	pool     *pgxpool.Pool

	stamp stamp.Stamp // the latest value passed to OnCommit.
	tx    pgx.Tx      // db transaction created by OnCommit.
}

var _ Events = (*serialEvents)(nil)

// AwaitConsistentPoint implements State.  It delegates to the loop.
func (e *serialEvents) AwaitConsistentPoint(
	ctx context.Context, comparison AwaitComparison, point stamp.Stamp,
) (stamp.Stamp, error) {
	return e.loop.AwaitConsistentPoint(ctx, comparison, point)
}

// Backfill implements Events. It delegates to the enclosing loop.
func (e *serialEvents) Backfill(
	ctx context.Context, source string, backfiller Backfiller, options ...Option,
) error {
	return e.loop.doBackfill(ctx, source, backfiller, options...)
}

// GetConsistentPoint implements State. It delegates to the loop.
func (e *serialEvents) GetConsistentPoint() stamp.Stamp { return e.loop.GetConsistentPoint() }

// GetTargetDB implements State. It delegates to the loop.
func (e *serialEvents) GetTargetDB() ident.Ident { return e.loop.GetTargetDB() }

// OnBegin implements Events.
func (e *serialEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	var err error
	if e.tx != nil {
		return errors.Errorf("OnBegin already called at %s", e.stamp)
	}
	e.stamp = point
	e.tx, err = e.pool.Begin(ctx)
	return errors.WithStack(err)
}

// OnCommit implements Events.
func (e *serialEvents) OnCommit(ctx context.Context) error {
	if e.tx == nil {
		return errors.New("OnCommit called without matching OnBegin")
	}

	err := e.tx.Commit(ctx)
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

// OnRollback implements Events and delegates to stop.
func (e *serialEvents) OnRollback(ctx context.Context, msg Message) error {
	if !IsRollback(msg) {
		return errors.New("the rollback message must be passed to OnRollback")
	}
	return e.stop(ctx)
}

// stop implements Events.
func (e *serialEvents) stop(_ context.Context) error {
	if e.tx != nil {
		_ = e.tx.Rollback(context.Background())
	}
	e.stamp = nil
	e.tx = nil
	return nil
}
