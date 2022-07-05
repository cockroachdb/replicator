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

package server

import (
	"net"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/google/wire"
)

type testFixture struct {
	*sinktest.Fixture
	Authenticator types.Authenticator
	Config        Config
	Listener      net.Listener
	Server        *Server
}

func newTestFixture(shouldUseWebhook) (*testFixture, func(), error) {
	panic(wire.Build(
		Set,
		cdc.Set,
		sinktest.TestSet,
		provideConnectionMode,
		provideTestConfig,
		wire.Struct(new(testFixture), "*"),
	))
}
