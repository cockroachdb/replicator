// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package perf contains wrappers for the service APIs.
package perf

import "github.com/cockroachdb/cdc-sink/internal/util/ident"

const (
	dbLabel     = "database"
	schemaLabel = "schema"
	tableLabel  = "table"
)

// Various "constants" used for configuring metrics.
var (
	objectives = map[float64]float64{
		.5:  .05,
		.95: .01,
		.99: .001,
	}
	schemaLabels = []string{dbLabel, schemaLabel}
	tableLabels  = []string{dbLabel, schemaLabel, tableLabel}
)

func schemaValues(schema ident.Schema) []string {
	return []string{schema.Database().Raw(), schema.Schema().Raw()}
}

func tableValues(table ident.Table) []string {
	return []string{table.Database().Raw(), table.Schema().Raw(), table.Table().Raw()}
}
