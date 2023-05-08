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
	"context"
	"net"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/cockroachdb/cdc-sink/internal/target/leases"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testFixture struct {
	Authenticator types.Authenticator
	Config        *Config
	Listener      net.Listener
	Pool          *pgxpool.Pool
	Server        *Server
	StagingDB     ident.StagingDB
	Watcher       types.Watchers
}

// We want this to be as close as possible to Start, it just exposes
// additional plumbing details via the returned testFixture pointer.
func newTestFixture(context.Context, *Config) (*testFixture, func(), error) {
	panic(wire.Build(
		Set,
		cdc.Set,
		leases.Set,
		logical.Set,
		script.Set,
		target.Set,
		wire.Bind(new(logical.Config), new(*Config)),
		wire.FieldsOf(new(*Config), "CDC"),
		wire.Struct(new(testFixture), "*"),
	))
}
