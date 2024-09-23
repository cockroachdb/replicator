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

package conveyor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mutationsErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mutations_error_count",
		Help: "the total number of mutations that encountered an error during processing",
	}, []string{"kind", "target"})
	mutationsReceivedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mutations_received_count",
		Help: "the total number of mutations received from the source",
	}, []string{"kind", "target"})
	mutationsSuccessCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mutations_success_count",
		Help: "the total number of mutations that were successfully processed",
	}, []string{"kind", "target"})
	sourceLagDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "source_lag_seconds",
		Help: "the age of the most recently received checkpoint",
	}, []string{"kind", "target"})
	targetLagDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "target_lag_seconds",
		Help: "the age of the data applied to the table",
	}, []string{"kind", "target"})
	resolvedMinTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "target_applied_timestamp_seconds",
		Help: "the wall time of the most recent applied resolved timestamp",
	}, []string{"kind", "target"})
	resolvedMaxTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "target_pending_timestamp_seconds",
		Help: "the wall time of the most recently received resolved timestamp",
	}, []string{"kind", "target"})
)
