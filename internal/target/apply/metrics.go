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

package apply

import (
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	applyConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_conflicts_total",
		Help: "the number of rows that experienced a CAS conflict",
	}, metrics.TableLabels)
	applyDeletes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_deletes_total",
		Help: "the number of rows deleted",
	}, metrics.TableLabels)
	applyDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "apply_duration_seconds",
		Help:    "the length of time it took to successfully apply mutations",
		Buckets: metrics.LatencyBuckets,
	}, metrics.TableLabels)
	applyErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_errors_total",
		Help: "the number of times an error was encountered while applying mutations",
	}, metrics.TableLabels)
	applyMutationAge = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "apply_mutation_age_seconds",
		Help: "the age of the mutation when it was applied; " +
			"the difference between the current wall time and the mutation's MVCC timestamp",
		Buckets: metrics.Buckets(1.0, 24*time.Hour.Seconds()),
	}, metrics.TableLabels)
	applyResolves = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_resolves_total",
		Help: "the number of rows that experienced a CAS conflict and which were resolved",
	}, metrics.TableLabels)
	applyUpserts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_upserts_total",
		Help: "the number of rows upserted",
	}, metrics.TableLabels)
)
