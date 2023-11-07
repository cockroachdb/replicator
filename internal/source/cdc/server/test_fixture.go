// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//go:build wireinject
// +build wireinject

package server

import (
	"context"
	"net"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/staging"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdserver"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/google/wire"
)

type testFixture struct {
	Authenticator types.Authenticator
	Config        *Config
	Diagnostics   *diag.Diagnostics
	Listener      net.Listener
	StagingPool   *types.StagingPool
	Server        *stdserver.Server
	StagingDB     ident.StagingSchema
	Stagers       types.Stagers
	Watcher       types.Watchers
}

// We want this to be as close as possible to Start, it just exposes
// additional plumbing details via the returned testFixture pointer.
func newTestFixture(*stopper.Context, *Config) (*testFixture, func(), error) {
	panic(wire.Build(
		Set,
		cdc.Set,
		diag.New,
		logical.Set,
		script.Set,
		staging.Set,
		target.Set,
		wire.Bind(new(context.Context), new(*stopper.Context)),
		wire.Bind(new(logical.Config), new(*Config)),
		wire.FieldsOf(new(*Config), "CDC"),
		wire.Struct(new(testFixture), "*"),
	))
}
