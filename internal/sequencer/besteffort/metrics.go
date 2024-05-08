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

package besteffort

import (
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	acceptAppliedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_accept_applied_count",
		Help: "number of mutations that were immediately applied",
	}, metrics.TableLabels)
	acceptDeferredCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_accept_deferred_count",
		Help: "number of mutations that were deferred for later sweeping",
	}, metrics.TableLabels)
	acceptDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "best_effort_accept_duration_seconds",
		Help:    "the length of time it took to initially apply or stage mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	acceptErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_accept_error_count",
		Help: "number of errors encountered while apply or staging for later sweeping",
	}, metrics.TableLabels)
)
