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

package script

import (
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	scriptEntryWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "script_entry_wait_seconds",
		Help:    "the length of time spent waiting to enter the JS runtime",
		Buckets: metrics.LatencyBuckets,
	})
	scriptExecTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "script_exec_time_seconds",
		Help:    "the length of time spent executing JS code",
		Buckets: metrics.LatencyBuckets,
	})
)
