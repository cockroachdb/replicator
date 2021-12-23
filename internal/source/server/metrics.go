// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpCodes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_status_codes_total",
		Help: "HTTP response code counts",
	}, []string{"code"})
	httpLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_latency_seconds",
		Help:    "the HTTP response latency for successful requests",
		Buckets: metrics.LatencyBuckets,
	})
	httpPayloadIn = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_payload_in_bytes_total",
		Help:    "the number HTTP payload body bytes read",
		Buckets: metrics.Buckets(1024, 10*1024*1024),
	})
)
