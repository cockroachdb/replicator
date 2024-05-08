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

package stage

import (
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	stageMergeLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stage_merge_lag_seconds",
		Help: "the difference between wall time and the table merge operator progress",
	}, metrics.SchemaLabels)
	stageMergeQueue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stage_merge_queue_count",
		Help: "the depth of the staging merge output queue",
	}, metrics.SchemaLabels)
	stageReadLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stage_read_lag_seconds",
		Help: "the difference between wall time and the table reader progress",
	}, metrics.TableLabels)
	stageReadRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_read_rows",
		Help: "the number of rows read from staging",
	}, metrics.TableLabels)
	stageReadDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stage_read_duration_seconds",
		Help:    "the length of time it took to successfully retrieve a page of staged mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	stageReadQueue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stage_read_queue_count",
		Help: "the depth of the table reader output queue",
	}, metrics.TableLabels)
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
	stageStaleMutations = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stage_stale_mutations_count",
		Help: "the number of un-applied staged mutations left after retiring applied mutations",
	}, metrics.TableLabels)
	stageCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_mutations_total",
		Help: "the number of mutations staged for this table",
	}, metrics.TableLabels)
	stageDuplicateCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_duplicate_mutations_total",
		Help: "the number of duplicate mutations received",
	}, metrics.TableLabels)
	stageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stage_duration_seconds",
		Help:    "the length of time it took to successfully stage mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	stageErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_errors_total",
		Help: "the number of times an error was encountered while staging mutations",
	}, metrics.TableLabels)
)
