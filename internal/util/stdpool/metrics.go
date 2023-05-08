// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stdpool

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	hostLabels = []string{"host"}

	poolAcquireCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_acquire_wait_count",
		Help: "the total number of times we waited to add a connection to the pool",
	}, hostLabels)
	poolAcquireDelay = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_acquire_wait_seconds",
		Help: "the total amount of time spent waiting for connection acquisition",
	}, hostLabels)
	poolAcquiredCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_acquired_connection_count",
		Help: "the number of in-use database connections",
	}, hostLabels)
	poolConstructingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_constructing_connection_count",
		Help: "the number of database connections that are being constructed",
	}, hostLabels)
	poolIdleCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_idle_connection_count",
		Help: "the number of idle database connections",
	}, hostLabels)
	poolMaxCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_max_connection_count",
		Help: "the maximum number of connections in the pool",
	}, hostLabels)
	poolDialErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_dial_error_count",
		Help: "the number of times a network connection could not be established",
	}, hostLabels)
	poolDialLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pool_dial_latency_seconds",
		Help:    "the number of seconds required to create a TCP connection to the database",
		Buckets: metrics.LatencyBuckets,
	}, hostLabels)
	poolDialSuccesses = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_dial_success_count",
		Help: "the number of times a network connection was created to the database",
	}, hostLabels)
	poolReadyLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pool_sql_ready_latency_seconds",
		Help:    "the number of seconds required to negotiate a SQL connection to the database",
		Buckets: metrics.LatencyBuckets,
	}, hostLabels)
)

// PublishMetrics creates prometheus metrics to export information
// about the given pool. The cancellation function must be called to
// deregister the metrics before the pool is shut down.
//
// Calling this function with a nil pointer is a no-op.
func PublishMetrics(pool *pgxpool.Pool) (cancel func()) {
	if pool == nil {
		return func() {}
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Update pool stats gauges at 1 QPS.
	go func() {
		labels := prometheus.Labels{
			"host": fmt.Sprintf("%s:%d",
				pool.Config().ConnConfig.Host, pool.Config().ConnConfig.Port),
		}

		acquireCount := poolAcquireCount.With(labels)
		acquireDelay := poolAcquireDelay.With(labels)
		acquiredCount := poolAcquiredCount.With(labels)
		constructingCount := poolConstructingCount.With(labels)
		idleCount := poolIdleCount.With(labels)
		maxCount := poolMaxCount.With(labels)

		// These metrics are reported to us as counters, so we need to
		// compute the deltas to pass them into the API.
		var prevAcquireCount int64
		var prevAcquireDuration time.Duration

		for {
			stat := pool.Stat()

			nextAcquireCount := stat.EmptyAcquireCount()
			acquireCount.Add(float64(nextAcquireCount - prevAcquireCount))
			prevAcquireCount = nextAcquireCount

			nextAcquireDuration := stat.AcquireDuration()
			acquireDelay.Add((nextAcquireDuration - prevAcquireDuration).Seconds())
			prevAcquireDuration = nextAcquireDuration

			acquiredCount.Set(float64(stat.AcquiredConns()))
			constructingCount.Set(float64(stat.ConstructingConns()))
			idleCount.Set(float64(stat.IdleConns()))
			maxCount.Set(float64(stat.MaxConns()))

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()

	return cancel
}
