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

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/staging"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/google/wire"
)

// Start creates a PostgreSQL logical replication loop using the
// provided configuration.
func Start(context.Context, *Config) ([]*logical.Loop, func(), error) {
	panic(wire.Build(
		wire.Bind(new(logical.Config), new(*Config)),
		ProvideFirestoreClient,
		ProvideLoops,
		ProvideTombstones,
		logical.Set,
		script.Set,
		staging.Set,
		target.Set,
	))
}

// Build remaining testable components from a common fixture.
func startLoopsFromFixture(*all.Fixture, *Config) ([]*logical.Loop, func(), error) {
	panic(wire.Build(
		wire.Bind(new(logical.Config), new(*Config)),
		wire.FieldsOf(new(*base.Fixture), "Context"),
		wire.FieldsOf(new(*all.Fixture),
			"Appliers", "Fixture", "Configs", "Memo", "Watchers"),
		ProvideFirestoreClient,
		ProvideLoops,
		ProvideTombstones,
		logical.Set,
		script.Set,
	))
}
