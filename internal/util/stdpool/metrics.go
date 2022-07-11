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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	hostLabels = []string{"host"}

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

	labels := prometheus.Labels{
		"host": fmt.Sprintf("%s:%d",
			pool.Config().ConnConfig.Host, pool.Config().ConnConfig.Port),
	}

	// Each call to Stat() triggers quite a bit of coordination overhead
	// within the pool implementation to count the connections. Given
	// the unpredictable nature of the callbacks below, we want to
	// clamp the maximum rate at which Stat() is called to 1 QPS.
	var lastSample time.Time
	var lastStat *pgxpool.Stat
	var statMu sync.Mutex
	sample := func() *pgxpool.Stat {
		statMu.Lock()
		defer statMu.Unlock()
		if time.Since(lastSample) <= time.Second {
			return lastStat
		}

		lastSample = time.Now()
		lastStat = pool.Stat()
		return lastStat
	}

	collectors := []prometheus.Collector{
		promauto.NewCounterFunc(prometheus.CounterOpts{
			Name:        "pool_acquire_wait_count",
			Help:        "the total number of times we waited to add a connection to the pool",
			ConstLabels: labels,
		}, func() float64 {
			return float64(sample().EmptyAcquireCount())
		}),
		promauto.NewCounterFunc(prometheus.CounterOpts{
			Name:        "pool_acquire_wait_seconds",
			Help:        "the total amount of time spent waiting for connection acquisition",
			ConstLabels: labels,
		}, func() float64 {
			return sample().AcquireDuration().Seconds()
		}),
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "pool_acquired_connection_count",
			Help:        "the number of in-use database connections",
			ConstLabels: labels,
		}, func() float64 {
			return float64(sample().AcquiredConns())
		}),
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "pool_constructing_connection_count",
			Help:        "the number of database connections that are being constructed",
			ConstLabels: labels,
		}, func() float64 {
			return float64(sample().ConstructingConns())
		}),
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "pool_idle_connection_count",
			Help:        "the number of idle database connections",
			ConstLabels: labels,
		}, func() float64 {
			return float64(sample().IdleConns())
		}),
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "pool_max_connection_count",
			Help:        "the maximum number of connections in the pool",
			ConstLabels: labels,
		}, func() float64 {
			return float64(sample().MaxConns())
		}),
	}

	return func() {
		for _, c := range collectors {
			prometheus.DefaultRegisterer.Unregister(c)
		}
	}
}
