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
