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

	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
)

// metricsEvents decorates an Events implementation with metrics.
type metricsEvents struct {
	Events
	point stamp.Stamp
}

var _ Events = (*metricsEvents)(nil)

func (e *metricsEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	e.point = point
	return e.Events.OnBegin(ctx, point)
}

func (e *metricsEvents) OnCommit(ctx context.Context) error {
	err := e.Events.OnCommit(ctx)
	if err != nil {
		commitFailureCount.Inc()
		return err
	}

	commitSuccessCount.Inc()
	if x, ok := e.point.(TimeStamp); ok {
		commitTime.Set(float64(x.AsTime().UnixNano()))
	}
	if x, ok := e.point.(OffsetStamp); ok {
		commitOffset.Set(float64(x.AsOffset()))
	}
	return nil
}
