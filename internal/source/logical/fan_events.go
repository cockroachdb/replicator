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

	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

// fanEvents is a high-throughput implementation of Events which
// does not preserve transaction boundaries.
type fanEvents struct {
	config *BaseConfig // The loop's configuration. Set by provider.
	fans   *fan.Fans   // Factory for fan-out behavior. Set by provider.
	loop   *loop       // The underlying loop.

	fan     *fan.Fan    // Created by OnBegin(), destroyed in stop().
	stamp   stamp.Stamp // The latest stamp passed into OnBegin().
	stopFan func()      // Called by stop() to ensure mutations have drained.
}

var _ Events = (*fanEvents)(nil)

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
func (f *fanEvents) OnBegin(_ context.Context, s stamp.Stamp) error {
	if f.fan == nil {
		var err error
		f.fan, f.stopFan, err = f.fans.New(
			f.config.ApplyTimeout,
			f.loop.setConsistentPoint,
			f.config.FanShards,
			f.config.BytesInFlight)
		if err != nil {
			return err
		}
	}
	f.stamp = s
	return nil
}

// OnCommit implements Events.
func (f *fanEvents) OnCommit(_ context.Context) error {
	if f.stamp == nil {
		return errors.New("OnCommit called without OnBegin")
	}
	// The fan will eventually call State.setConsistentPoint.
	err := f.fan.Mark(f.stamp)
	f.stamp = nil
	return err
}

// OnData implements Events and delegates to the enclosed fan.
func (f *fanEvents) OnData(
	ctx context.Context, _ ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	if f.stamp == nil {
		return errors.New("OnData called without OnBegin")
	}
	return f.fan.Enqueue(ctx, f.stamp, target, muts)
}

// OnRollback implements Events and resets the enclosed fan.
func (f *fanEvents) OnRollback(_ context.Context, msg Message) error {
	if !IsRollback(msg) {
		return errors.New("the rollback message must be passed to OnRollback")
	}
	// Dump any in-flight mutations, but keep the fan running.
	if f.fan != nil {
		f.fan.Reset()
	}
	f.stamp = nil
	return nil
}

// setConsistentPoint implements State. It delegates to the loop.
func (f *fanEvents) setConsistentPoint(s stamp.Stamp) { f.loop.setConsistentPoint(s) }

// reset implements Events.
func (f *fanEvents) stop() {
	if f.fan != nil {
		// Shut down the fan and wait for it to have stopped.
		f.stopFan()
		<-f.fan.Stopped()
	}
	f.fan = nil
	f.stamp = nil
	f.stopFan = nil
}
