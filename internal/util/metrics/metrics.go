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

// Package metrics contains some common utility functions for
// constructing performance-monitoring metrics.
package metrics

import (
	"math"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	schemaLabel = "schema"
	tableLabel  = "table"
)

var (
	// LatencyBuckets is a default collection of histogram buckets
	// for latency metrics. The values in this slice assume that the
	// metric's base units are measured in seconds.
	LatencyBuckets = Buckets(time.Millisecond.Seconds(), time.Minute.Seconds())
	// SchemaLabels are the labels to be applied to single-schema vector
	// metrics.
	SchemaLabels = []string{schemaLabel}
	// TableLabels are the labels to be applied to table-specific,
	// vector metrics.
	TableLabels = []string{schemaLabel, tableLabel}
)

// init publishes a build-info metric and start time.
func init() {
	if bi, ok := debug.ReadBuildInfo(); ok {
		labels := prometheus.Labels{
			"go_version": bi.GoVersion,
			"module":     bi.Path,
		}
		for _, setting := range bi.Settings {
			switch setting.Key {
			case "vcs.revision":
				labels["commit"] = setting.Value
			case "vcs.time":
				labels["build_time"] = setting.Value
			}
		}
		promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "cdc_sink_info",
			Help:        "information about the cdc-sink binary",
			ConstLabels: labels,
		}).Set(1)
	}
	promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_sink_start_seconds",
		Help: "the wall time at which cdc-sink was started",
	}).SetToCurrentTime()
}

// Buckets computes a linear log10 sequence of buckets, starting
// from the base unit, up to the specified maximum.
func Buckets(base, max float64) []float64 {
	var ret []float64
	for {
		for i := 0; i < 9; i++ {
			// next = i*base + base
			next := math.FMA(float64(i), base, base)
			if next > max {
				return ret
			}
			// Round to three decimal places to avoid awkward mantissas.
			next = math.Round(next*1000) / 1000
			ret = append(ret, next)
		}
		base *= 10
	}
}

// SchemaValues returns the values to plug into a vector metric
// that expects SchemaLabels.
func SchemaValues(sch ident.Schema) []string {
	return []string{sch.Raw()}
}

// TableValues returns the values to plug into a vector metric
// that expects TableLabels.
func TableValues(table ident.Table) []string {
	return []string{table.Schema().Raw(), table.Table().Raw()}
}
