// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package staging contains all services which interact with the staging
// database.
package staging

import (
	"github.com/cockroachdb/cdc-sink/internal/staging/leases"
	"github.com/cockroachdb/cdc-sink/internal/staging/memo"
	"github.com/cockroachdb/cdc-sink/internal/staging/stage"
	"github.com/google/wire"
)

// Set is used by Wire and contains providers for all target
// sub-packages.
var Set = wire.NewSet(
	leases.Set,
	memo.Set,
	stage.Set,
)
