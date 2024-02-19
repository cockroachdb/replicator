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
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
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
	sweepActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "best_effort_sweep_active_bool",
		Help: "non-zero if this instance of cdc-sink is processing the table",
	}, metrics.TableLabels)
	sweepAbandonedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_sweep_abandoned_count",
		Help: "number of sweep attempts that were abandoned early due to too many un-appliable mutations",
	}, metrics.TableLabels)
	sweepAttemptedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_sweep_attempt_count",
		Help: "number of mutations found during cleanup sweep",
	}, metrics.TableLabels)
	sweepAppliedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_sweep_apply_count",
		Help: "number of mutations applied during cleanup sweep",
	}, metrics.TableLabels)
	sweepDeferrals = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_sweep_deferral_count",
		Help: "applies that were deferred during cleanup sweeps due to FK constraints",
	}, metrics.TableLabels)
	sweepErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "best_effort_sweep_error_count",
		Help: "applies that errored out during cleanup sweeps",
	}, metrics.TableLabels)
	sweepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "best_effort_sweep_duration_seconds",
		Help:    "the length of time it took to look for and apply staged mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	sweepLastAttempt = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "best_effort_sweep_attempt_timestamp_seconds",
		Help: "the wall time at which a sweep attempt was last tried",
	}, metrics.TableLabels)
)
