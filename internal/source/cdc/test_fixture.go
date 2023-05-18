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

package cdc

import (
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/staging/leases"
	"github.com/google/wire"
)

type testFixture struct {
	*all.Fixture
	Handler   *Handler
	Resolvers *Resolvers
}

func newTestFixture(*all.Fixture, *Config) (*testFixture, func(), error) {
	panic(wire.Build(
		Set,
		wire.FieldsOf(new(*base.Fixture), "Context"),
		wire.FieldsOf(new(*all.Fixture),
			"Appliers", "Fixture", "Stagers", "Watchers"),
		leases.Set,
		logical.Set,
		script.Set,
		trust.New, // Is valid to use as a provider.
		wire.Struct(new(testFixture), "*"),
		wire.Bind(new(logical.Config), new(*Config)),
	))
}
