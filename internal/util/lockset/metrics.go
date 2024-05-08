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

package lockset

import (
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	labels = []string{"set"}

	deferredStart = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lockset_deferred_start_count",
		Help: "the number of tasks that had to wait for another task before being started",
	}, labels)
	execCompleteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "lockset_exec_complete_seconds",
		Help:    "the elapsed time between a task being enqueued and when it finished executing",
		Buckets: metrics.LatencyBuckets,
	}, labels)
	execWaitTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "lockset_exec_wait_seconds",
		Help:    "the elapsed time between a task being enqueued and when it began executing",
		Buckets: metrics.LatencyBuckets,
	}, labels)
	immediateStart = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lockset_immediate_start_count",
		Help: "the number of tasks that could be started without waiting for other tasks",
	}, labels)
	retriedTasks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lockset_retried_tasks_count",
		Help: "the number of tasks were retried at the head of the global queue",
	}, labels)
)
