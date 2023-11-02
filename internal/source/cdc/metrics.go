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

package cdc

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	schemaLabels = []string{"schema"}

	committedAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_resolver_committed_age_seconds",
		Help: "the age of the committed resolved timestamp or " +
			"zero if this is not the resolving instance",
	}, schemaLabels)
	committedTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_resolver_committed_time_seconds",
		Help: "the wall time of the committed resolved timestamp or " +
			"zero if this is not the resolving instance",
	}, schemaLabels)
	flushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_resolver_flush_duration_seconds",
		Help:    "the amount of time it took to flush an individual batch of mutations",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	markDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_resolver_mark_duration_seconds",
		Help:    "the amount of time it took to mark an incoming resolved timestamp",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	processDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_resolver_process_duration_seconds",
		Help:    "the amount of time it took to fully process a resolved timestamp",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	processing = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_resolver_processing_boolean",
		Help: "1 if this instance of cdc-sink is processing resolved timestamps for the schema",
	}, schemaLabels)
	proposedAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_resolver_proposed_age_seconds",
		Help: "the age of the proposed resolved timestamp or " +
			"zero if this is not the resolving instance",
	}, schemaLabels)
	proposedTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_resolver_proposed_time_seconds",
		Help: "the wall time of the proposed resolved timestamp or " +
			"zero if this is not the resolving instance",
	}, schemaLabels)
	recordDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_resolver_record_duration_seconds",
		Help:    "the amount of time it took to record a completed resolved timestamp",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
	selectTimestampDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_resolver_select_timestamp_duration_seconds",
		Help:    "the amount of time it took to find the next timestamp to process",
		Buckets: metrics.LatencyBuckets,
	}, schemaLabels)
)
