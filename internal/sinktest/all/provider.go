// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package all contains a test rig for all services.
package all

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/staging"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/google/wire"
)

// TestSet contains providers to create a self-contained Fixture.
var TestSet = wire.NewSet(
	base.TestSet,
	staging.Set,
	target.Set,

	ProvideWatcher,

	wire.Struct(new(Fixture), "*"),
)

// ProvideWatcher is called by Wire to construct a Watcher
// bound to the testing database.
func ProvideWatcher(
	ctx context.Context, testDB base.TestDB, watchers types.Watchers,
) (types.Watcher, error) {
	return watchers.Get(ctx, testDB.Ident())
}
