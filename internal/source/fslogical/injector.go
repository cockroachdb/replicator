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
		ProvideScriptTarget,
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
			"Appliers", "Fixture", "Configs", "Memo", "VersionChecker", "Watchers"),
		ProvideFirestoreClient,
		ProvideLoops,
		ProvideScriptTarget,
		ProvideTombstones,
		logical.Set,
		script.Set,
	))
}
