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

package script

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/google/wire"
)

// Evaluate the loaded script.
func Evaluate(
	ctx context.Context,
	loader *Loader,
	configs *applycfg.Configs,
	diags *diag.Diagnostics,
	targetSchema TargetSchema,
	watchers types.Watchers,
) (*UserScript, error) {
	panic(wire.Build(ProvideUserScript))
}

func newScriptFromFixture(*all.Fixture, *Config, TargetSchema) (*UserScript, error) {
	panic(wire.Build(
		Set,
		wire.FieldsOf(new(*all.Fixture), "Diagnostics", "Fixture", "Configs", "Watchers"),
		wire.FieldsOf(new(*base.Fixture), "Context"),
	))
}
