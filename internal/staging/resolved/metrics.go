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

package resolved

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	schemaLabels = []string{"schema"}

	// TODO(bob): Remove the TEMP_ prefix when the cdc package is reworked.
	// The prometheus library panics when multiple metrics are registered
	// with the same name as we have here and in the cdc package.

	committedAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "TEMP_cdc_resolver_committed_age_seconds",
		Help: "the age of the committed resolved timestamp",
	}, schemaLabels)
	committedTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "TEMP_cdc_resolver_committed_time_seconds",
		Help: "the wall time of the committed resolved timestamp",
	}, schemaLabels)
	markDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "TEMP_cdc_resolver_mark_duration_seconds",
		Help:    "the amount of time it took to mark an incoming resolved timestamp",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	proposedAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "TEMP_cdc_resolver_proposed_age_seconds",
		Help: "the age of the proposed resolved timestamp",
	}, schemaLabels)
	proposedTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "TEMP_cdc_resolver_proposed_time_seconds",
		Help: "the wall time of the proposed resolved timestamp",
	}, schemaLabels)
	recordDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "TEMP_cdc_resolver_record_duration_seconds",
		Help:    "the amount of time it took to record a completed resolved timestamp",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	refreshDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "TEMP_cdc_resolver_refresh_duration_seconds",
		Help:    "the amount of time it took to refresh resolved timestamp range",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
)
