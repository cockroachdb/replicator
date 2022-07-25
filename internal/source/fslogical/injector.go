// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build wireinject
// +build wireinject

package fslogical

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/google/wire"
)

// Start creates a PostgreSQL logical replication loop using the
// provided configuration.
func Start(context.Context, *Config) ([]*logical.Loop, func(), error) {
	panic(wire.Build(
		ProvideBaseConfig,
		ProvideFireStoreClient,
		ProvideLoops,
		logical.Set,
		target.Set,
	))
}
