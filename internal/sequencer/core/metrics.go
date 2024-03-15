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

package core

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	sweepActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "core_sweep_active_bool",
		Help: "non-zero if this instance of cdc-sink is processing the schema",
	}, metrics.TableLabels)
	sweepAppliedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "core_sweep_apply_count",
		Help: "number of mutations applied during sweep",
	}, metrics.SchemaLabels)
	sweepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "core_sweep_duration_seconds",
		Help:    "the length of time it took to look for and apply staged mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.SchemaLabels)
	sweepLastAttempt = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "core_sweep_attempt_timestamp_seconds",
		Help: "the wall time at which a sweep attempt was last tried",
	}, metrics.SchemaLabels)
	sweepLastSuccess = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "core_sweep_success_timestamp_seconds",
		Help: "the wall time at which a sweep attempt last succeeded",
	}, metrics.SchemaLabels)
	sweepSkewCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "core_sweep_skew_count",
		Help: "the number of times a target transaction committed, but the staging tx did not",
	}, metrics.SchemaLabels)
)
