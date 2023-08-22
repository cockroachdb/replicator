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
			"Appliers", "Configs", "Fixture", "Memo", "Stagers", "VersionChecker", "Watchers"),
		leases.Set,
		logical.Set,
		script.Set,
		trust.New, // Is valid to use as a provider.
		wire.Struct(new(testFixture), "*"),
		wire.Bind(new(logical.Config), new(*Config)),
	))
}
