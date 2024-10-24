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

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stdserver"
	"github.com/google/wire"
)

type testFixture struct {
	Authenticator types.Authenticator
	Config        *Config
	Diagnostics   *diag.Diagnostics
	Listener      net.Listener
	Memo          types.Memo
	StagingPool   *types.StagingPool
	Server        *stdserver.Server
	StagingDB     ident.StagingSchema
	Stagers       types.Stagers
	Watcher       types.Watchers
}

// We want this to be as close as possible to NewServer, it just exposes
// additional plumbing details via the returned testFixture pointer.
func newTestFixture(*stopper.Context, *Config) (*testFixture, func(), error) {
	panic(wire.Build(
		completeSet,
		wire.Bind(new(context.Context), new(*stopper.Context)),
		wire.FieldsOf(new(*Config), "CDC", "Stage", "Staging", "Target"),
		wire.Struct(new(testFixture), "*"),
	))
}
