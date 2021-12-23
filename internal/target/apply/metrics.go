// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	applyDeletes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_deletes_total",
		Help: "the number of rows deleted",
	}, metrics.TableLabels)
	applyDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "apply_duration_seconds",
		Help:    "the length of time it took to successfully apply mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	applyErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_errors_total",
		Help: "the number of times an error was encountered while applying mutations",
	}, metrics.TableLabels)
	applyUpserts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_upserts_total",
		Help: "the number of rows upserted",
	}, metrics.TableLabels)
)
