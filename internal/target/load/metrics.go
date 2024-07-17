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

package load

import (
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	loadDirty = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "load_dirty_count",
		Help: "the number of property bags that were populated with data from the target table",
	}, metrics.TableLabels)
	loadNotFound = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "load_not_found_count",
		Help: "the number of property bags that could not be found in the target table",
	}, metrics.TableLabels)
	loadUnmodified = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "load_unmodified_count",
		Help: "the number of property bags that were already fully-populated",
	}, metrics.TableLabels)
	loadOverallDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "load_overall_duration_seconds",
		Help: "the end-to-end wall time needed to process property bags",
	}, metrics.TableLabels)
	loadQueryDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "load_query_duration_seconds",
		Help: "the wall time consumed by database queries",
	}, metrics.TableLabels)
)
