// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package target contains various services for performing operations on
// the target database cluster.
package target

import (
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/target/memo"
	"github.com/cockroachdb/cdc-sink/internal/target/resolve"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/script"
	"github.com/cockroachdb/cdc-sink/internal/target/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/google/wire"
)

// Set is used by Wire and contains providers for all target
// sub-packages.
var Set = wire.NewSet(
	apply.Set,
	fan.Set,
	memo.Set,
	resolve.Set,
	schemawatch.Set,
	script.Set,
	stage.Set,
	timekeeper.Set,
)
