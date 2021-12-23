// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timekeeper

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	tkDurations = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "timekeeper_duration_seconds",
		Help:    "the length of time it took to successfully record resolved timestamps",
		Buckets: metrics.LatencyBuckets,
	})
	tkErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "timekeeper_errors_total",
		Help: "the number of times an error was encountered while recording resolved timestamps",
	})
	tkResolved = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "timekeeper_schema_resolved_seconds",
		Help: "the last-known resolved timestamp that has been published for a database schema",
	}, metrics.SchemaLabels)
)
