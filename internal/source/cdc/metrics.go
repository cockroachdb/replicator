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

package cdc

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mutationsErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_mutations_error_count",
		Help: "the total number of mutations that encountered an error during processing",
	}, []string{"target"})
	mutationsReceivedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_mutations_received_count",
		Help: "the total number of mutations received from the source",
	}, []string{"target"})
	mutationsSuccessCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_mutations_success_count",
		Help: "the total number of mutations that were successfully processed",
	}, []string{"target"})
	sourceLagDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_source_lag_seconds",
		Help: "the age of the data received from the source changefeed",
	}, []string{"target"})
	targetLagDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_target_lag_seconds",
		Help: "the age of the data applied to the table",
	}, []string{"target"})
	resolvedMinTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_target_applied_timestamp_seconds",
		Help: "the wall time of the most recent applied resolved timestamp",
	}, []string{"target"})
	resolvedMaxTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_target_pending_timestamp_seconds",
		Help: "the wall time of the most recently received resolved timestamp",
	}, []string{"target"})
)
