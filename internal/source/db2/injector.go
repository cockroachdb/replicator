// Copyright 2024 The Cockroach Authors
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

package db2

import (
	"context"

	scriptRuntime "github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/chaos"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/immediate"
	scriptSequencer "github.com/cockroachdb/cdc-sink/internal/sequencer/script"
	"github.com/cockroachdb/cdc-sink/internal/sinkprod"
	"github.com/cockroachdb/cdc-sink/internal/staging"
	"github.com/cockroachdb/cdc-sink/internal/target"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/google/wire"
)

// Start creates a DB2 logical replication loop using the
// provided configuration.
func Start(ctx *stopper.Context, config *Config) (*DB2, error) {
	panic(wire.Build(
		wire.Bind(new(context.Context), new(*stopper.Context)),
		wire.Struct(new(DB2), "*"),
		wire.FieldsOf(new(*Config), "Script"),
		wire.FieldsOf(new(*EagerConfig), "DLQ", "Sequencer", "Staging", "Target"),
		Set,
		immediate.Set,
		chaos.Set,
		diag.New,
		scriptRuntime.Set,
		scriptSequencer.Set,
		sinkprod.Set,
		staging.Set,
		target.Set,
	))
}
