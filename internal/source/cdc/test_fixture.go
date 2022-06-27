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
	"github.com/cockroachdb/cdc-sink/internal/target/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/google/wire"
)

type testFixture struct {
	*sinktest.Fixture
	Handler *Handler
}

func newTestFixture() (*testFixture, func(), error) {
	panic(wire.Build(
		Set,
		sinktest.TestSet,
		trust.New, // Is valid to use as a provider.
		wire.Struct(new(testFixture), "*"),
	))
}
