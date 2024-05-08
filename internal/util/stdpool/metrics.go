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

package stdpool

import (
	"context"
	"database/sql"
	"net"
	"time"

	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

var (
	labels        = []string{"pool"}
	metricsDialer = &net.Dialer{
		KeepAlive: 5 * time.Minute,
		Timeout:   10 * time.Second,
	}

	poolAcquireCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_acquire_wait_count",
		Help: "the total number of times we waited to add a connection to the pool",
	}, labels)
	poolAcquireDelay = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_acquire_wait_seconds",
		Help: "the total amount of time spent waiting for connection acquisition",
	}, labels)
	poolAcquiredCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_acquired_connection_count",
		Help: "the number of in-use database connections",
	}, labels)
	poolConstructingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_constructing_connection_count",
		Help: "the number of database connections that are being constructed",
	}, labels)
	poolIdleCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_idle_connection_count",
		Help: "the number of idle database connections",
	}, labels)
	poolMaxCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pool_max_connection_count",
		Help: "the maximum number of connections in the pool",
	}, labels)
	poolDialErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_dial_error_count",
		Help: "the number of times a network connection could not be established",
	}, labels)
	poolDialLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pool_dial_latency_seconds",
		Help:    "the number of seconds required to create a TCP connection to the database",
		Buckets: metrics.LatencyBuckets,
	}, labels)
	poolDialSuccesses = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pool_dial_success_count",
		Help: "the number of times a network connection was created to the database",
	}, labels)
	poolReadyLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pool_sql_ready_latency_seconds",
		Help:    "the number of seconds required to negotiate a SQL connection to the database",
		Buckets: metrics.LatencyBuckets,
	}, labels)
)

// WithMetrics publishes prometheus metrics, using the given pool name.
func WithMetrics(pool string) Option {
	return &withMetrics{
		labels: prometheus.Labels{"pool": pool},
	}
}

type withMetrics struct{ labels prometheus.Labels }

func (o *withMetrics) option() {}

// pgxPoolConfig provides metrics around database connections. We use a
// standard net.Dialer (per defaults from pgx) and measure both how long
// it takes to create the network connection and how long it takes to
// have a fully-functioning SQL connection.  This would allow users to
// distinguish between dis-function in the network or the target
// database.
func (o *withMetrics) pgxPoolConfig(_ context.Context, cfg *pgxpool.Config) error {
	type timedConn struct {
		net.Conn
		start time.Time
	}

	dialErrors := poolDialErrors.With(o.labels)
	dialLatency := poolDialLatency.With(o.labels)
	dialSuccesses := poolDialSuccesses.With(o.labels)
	readyLatency := poolReadyLatency.With(o.labels)
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()
		conn, err := metricsDialer.DialContext(ctx, network, addr)
		if err == nil {
			dialSuccesses.Inc()
			dialLatency.Observe(time.Since(start).Seconds())
			conn = &timedConn{conn, start}
		} else {
			dialErrors.Inc()
		}
		return conn, err
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if timed, ok := conn.PgConn().Conn().(*timedConn); ok {
			readyLatency.Observe(time.Since(timed.start).Seconds())
		}
		return nil
	}
	return nil
}
func (o *withMetrics) pgxPool(ctx context.Context, pool *pgxpool.Pool) error {
	o.poolMetrics(ctx, pool)
	return nil
}
func (o *withMetrics) sqlDB(ctx context.Context, db *sql.DB) error {
	o.poolMetrics(ctx, db)
	return nil
}

// poolMetrics creates prometheus metrics to export information about
// the given pool. The cancellation function must be called to
// deregister the metrics before the pool is shut down.
func (o *withMetrics) poolMetrics(ctx context.Context, pool any) {
	acquireCount := poolAcquireCount.With(o.labels)
	acquireDelay := poolAcquireDelay.With(o.labels)
	acquiredCount := poolAcquiredCount.With(o.labels)
	constructingCount := poolConstructingCount.With(o.labels)
	idleCount := poolIdleCount.With(o.labels)
	maxCount := poolMaxCount.With(o.labels)

	// Update pool stats gauges at 1 QPS.
	stop := stopper.From(ctx)
	stop.Go(func() error {
		// These metrics are reported to us as counters, so we need to
		// compute the deltas to pass them into the API.
		var prevAcquireCount int64
		var prevAcquireDuration time.Duration

		for {
			switch t := pool.(type) {
			case *pgxpool.Pool:
				stat := t.Stat()

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

			case *sql.DB:
				stat := t.Stats()

				nextAcquireCount := stat.WaitCount
				acquireCount.Add(float64(nextAcquireCount - prevAcquireCount))
				prevAcquireCount = nextAcquireCount

				nextAcquireDuration := stat.WaitDuration
				acquireDelay.Add((nextAcquireDuration - prevAcquireDuration).Seconds())
				prevAcquireDuration = nextAcquireDuration

				acquiredCount.Set(float64(stat.InUse))
				idleCount.Set(float64(stat.Idle))
				maxCount.Set(float64(stat.MaxOpenConnections))

			default:
				log.Warnf("cannot export database pool metrics for %T", t)
				return nil
			}

			select {
			case <-stop.Stopping():
				return nil
			case <-time.After(time.Second):
			}
		}
	})
}
