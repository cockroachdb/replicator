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

package mylogical

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/staging"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/google/wire"
)

// MYLogical isa MySQL/MariaDB logical replication loop.
type MYLogical struct {
	Diagnostics *diag.Diagnostics
	Loop        *logical.Loop
}

// Start creates a MySQL/MariaDB logical replication loop using the
// provided configuration.
func Start(ctx context.Context, config *Config) (*MYLogical, func(), error) {
	panic(wire.Build(
		wire.Bind(new(logical.Config), new(*Config)),
		wire.Struct(new(MYLogical), "*"),
		Set,
		diag.New,
		logical.Set,
		script.Set,
		staging.Set,
		target.Set,
	))
}
