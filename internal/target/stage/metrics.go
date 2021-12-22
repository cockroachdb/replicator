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
	stageDrainCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_drain_mutations_total",
		Help: "the number of mutations drained for this table",
	}, metrics.TableLabels)
	stageDrainDurations = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "stage_drain_duration_seconds",
		Help:       "the length of time it took to successfully drain mutations",
		Objectives: metrics.Objectives,
	}, metrics.TableLabels)
	stageDrainErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_drain_errors_total",
		Help: "the number of times an error was encountered while draining mutations",
	}, metrics.TableLabels)

	stageStoreCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_store_mutations_total",
		Help: "the number of mutations stored for this table",
	}, metrics.TableLabels)
	stageStoreDurations = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "stage_store_duration_seconds",
		Help:       "the length of time it took to successfully store mutations",
		Objectives: metrics.Objectives,
	}, metrics.TableLabels)
	stageStoreErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_store_errors_total",
		Help: "the number of times an error was encountered while storing mutations",
	}, metrics.TableLabels)
)
