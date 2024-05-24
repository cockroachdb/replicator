// Copyright 2024 The Cockroach Authors
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

package objstore

import (
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	topicLabels = []string{"topic"}
)
var (
	applyDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "objstore_apply_seconds",
		Help:    "the time spent in applying a batch of files",
		Buckets: metrics.LatencyBuckets,
	}, topicLabels)
	batchSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "objstore_batch_size",
		Help: "the number files between two consecutive resolved timestamps",
	}, topicLabels)
	fetchResolvedDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "objstore_fetch_resolved_seconds",
		Help:    "the time spent in fetching resolved timestamps",
		Buckets: metrics.LatencyBuckets,
	}, topicLabels)
	processDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "objstore_process_seconds",
		Help:    "the time spent in processing one ndjson file",
		Buckets: metrics.LatencyBuckets,
	}, topicLabels)
	retryCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "objstore_retry_count",
		Help: "the total number of times we are retrying an operation",
	}, []string{"operation"})
)
