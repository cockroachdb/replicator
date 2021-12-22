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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpCodes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_status_codes_total",
		Help: "the HTTP response latency",
	}, []string{"code"})
	httpLatency = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "http_latency_seconds",
		Help: "the HTTP response latency for successful requests",
	})
	httpPayloadIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "http_payload_in_bytes_total",
		Help: "the number HTTP payload body bytes read",
	})
)
