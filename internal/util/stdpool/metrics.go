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
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
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
