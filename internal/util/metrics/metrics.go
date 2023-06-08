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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

const (
	dbLabel     = "database"
	schemaLabel = "schema"
	tableLabel  = "table"
)

var (
	// LatencyBuckets is a default collection of histogram buckets
	// for latency metrics. The values in this slice assume that the
	// metric's base units are measured in seconds.
	LatencyBuckets = Buckets(time.Millisecond.Seconds(), time.Minute.Seconds())
	// SchemaLabels are the labels to be applied to schema-specific,
	// vector metrics.
	SchemaLabels = []string{dbLabel, schemaLabel}
	// TableLabels are the labels to be applied to table-specific,
	// vector metrics.
	TableLabels = []string{dbLabel, schemaLabel, tableLabel}
)

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
func SchemaValues(schema ident.Schema) []string {
	return []string{schema.Database().Raw(), schema.Schema().Raw()}
}

// TableValues returns the values to plug into a vector metric
// that expects TableLabels.
func TableValues(table ident.Table) []string {
	return []string{table.Database().Raw(), table.Schema().Raw(), table.Table().Raw()}
}
