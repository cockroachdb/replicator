// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stage

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	stageRetireDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stage_retire_duration_seconds",
		Help:    "the length of time it took to successfully retire applied mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	stageRetireErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_retire_errors_total",
		Help: "the number of times an error was encountered while retiring mutations",
	}, metrics.TableLabels)

	stageSelectCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_select_mutations_total",
		Help: "the number of mutations read for this table",
	}, metrics.TableLabels)
	stageSelectDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stage_select_duration_seconds",
		Help:    "the length of time it took to successfully select mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	stageSelectErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_select_errors_total",
		Help: "the number of times an error was encountered while selecting mutations",
	}, metrics.TableLabels)

	stageStoreCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_store_mutations_total",
		Help: "the number of mutations stored for this table",
	}, metrics.TableLabels)
	stageStoreDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stage_store_duration_seconds",
		Help:    "the length of time it took to successfully store mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	stageStoreErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_store_errors_total",
		Help: "the number of times an error was encountered while storing mutations",
	}, metrics.TableLabels)
)
