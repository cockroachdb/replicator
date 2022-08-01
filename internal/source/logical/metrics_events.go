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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	loopLabels = []string{"loop"}

	backfillStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logical_backfill_status",
		Help: "this is set to one if the logical loop is in backfill mode",
	}, loopLabels)
	commitSuccessCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logical_commit_success_total",
		Help: "the number transactions from the source database that were applied",
	}, loopLabels)
	commitFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logical_commit_failure_total",
		Help: "the number transactions from the source database that failed to apply",
	}, loopLabels)
	commitOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logical_last_commit_offset_bytes",
		Help: "the offset that we are reporting to the source database",
	}, loopLabels)
	commitTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logical_last_commit_seconds",
		Help: "the original time of the most recently applied commit from the source database",
	}, loopLabels)
)

// metricsEvents decorates an Events implementation with metrics.
type metricsEvents struct {
	Events
	point stamp.Stamp

	metrics struct {
		commitSuccess prometheus.Counter
		commitFailure prometheus.Counter
		commitOffset  prometheus.Gauge
		commitTime    prometheus.Gauge
	}
}

var _ Events = (*metricsEvents)(nil)

func (e *metricsEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	e.point = point
	return e.Events.OnBegin(ctx, point)
}

func (e *metricsEvents) OnCommit(ctx context.Context) error {
	err := e.Events.OnCommit(ctx)
	if err != nil {
		e.metrics.commitFailure.Inc()
		return err
	}

	e.metrics.commitSuccess.Inc()
	if x, ok := e.point.(TimeStamp); ok {
		e.metrics.commitTime.Set(float64(x.AsTime().UnixNano()))
	}
	if x, ok := e.point.(OffsetStamp); ok {
		e.metrics.commitOffset.Set(float64(x.AsOffset()))
	}
	return nil
}

func (e *metricsEvents) withLoopName(name string) *metricsEvents {
	e.metrics.commitSuccess = commitSuccessCount.WithLabelValues(name)
	e.metrics.commitFailure = commitFailureCount.WithLabelValues(name)
	e.metrics.commitOffset = commitOffset.WithLabelValues(name)
	e.metrics.commitTime = commitTime.WithLabelValues(name)
	return e
}
