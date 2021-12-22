// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package metrics contains some common utility functions for
// constructing performance-monitoring metrics.
package metrics

import "github.com/cockroachdb/cdc-sink/internal/util/ident"

const (
	dbLabel     = "database"
	schemaLabel = "schema"
	tableLabel  = "table"
)

var (
	// Objectives is a map that contains a default set of percentile
	// buckets for summary-type metrics.
	Objectives = map[float64]float64{
		.5:  .05,
		.95: .01,
		.99: .001,
	}
	// SchemaLabels are the labels to be applied to schema-specific,
	// vector metrics.
	SchemaLabels = []string{dbLabel, schemaLabel}
	// TableLabels are the labels to be applied to table-specific,
	// vector metrics.
	TableLabels = []string{dbLabel, schemaLabel, tableLabel}
)

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
