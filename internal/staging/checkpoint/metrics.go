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

package checkpoint

import (
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	schemaLabels = []string{"schema"}

	advanceDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "checkpoint_advance_duration_seconds",
		Help:    "the amount of time it took to record an incoming checkpoint",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	commitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "checkpoint_commit_duration_seconds",
		Help:    "the amount of time it took to save the committed checkpoint time",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	committedAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "checkpoint_committed_age_seconds",
		Help: "the age of the committed checkpoint",
	}, schemaLabels)
	committedTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "checkpoint_committed_time_seconds",
		Help: "the wall time of the committed checkpoint",
	}, schemaLabels)
	proposedAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "checkpoint_proposed_age_seconds",
		Help: "the age of the proposed checkpoint",
	}, schemaLabels)
	proposedGoingBackwards = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "checkpoint_proposed_going_backwards_errors",
		Help: "this counter indicates an error condition where the changefeed has been restarted",
	}, schemaLabels)
	proposedTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "checkpoint_proposed_time_seconds",
		Help: "the wall time of the proposed resolved timestamp",
	}, schemaLabels)
	refreshDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "checkpoint_refresh_duration_seconds",
		Help:    "the amount of time it took to refresh the checkpoint range",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
)
