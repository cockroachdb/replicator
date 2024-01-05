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

package db2

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	batchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "db2_batch_latency",
		Help:    "the length of time it took to process a batch",
		Buckets: metrics.LatencyBuckets,
	})
	batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "db2_batch_size",
		Help:    "the size of batch",
		Buckets: metrics.Buckets(1, 10000),
	})
	mutationCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db2_mutation_total",
			Help: "Total number of mutations by source table.",
		},
		[]string{"table", "op"},
	)
	queryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "db2_query_latency",
		Help:    "the length of time it took to process a query",
		Buckets: metrics.LatencyBuckets,
	}, []string{"table"})
	rollbackBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "db2_rollback_size",
		Help:    "the size of a rollback",
		Buckets: metrics.Buckets(1, 10000),
	})
)
