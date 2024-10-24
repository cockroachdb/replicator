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

package pglogical

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	scriptRuntime "github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/decorators"
	"github.com/cockroachdb/replicator/internal/sequencer/immediate"
	scriptSequencer "github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/staging"
	"github.com/cockroachdb/replicator/internal/target"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/google/wire"
)

// Start creates a PostgreSQL logical replication loop using the
// provided configuration.
func Start(*stopper.Context, *Config) (*PGLogical, error) {
	panic(wire.Build(
		wire.Bind(new(context.Context), new(*stopper.Context)),
		wire.Struct(new(PGLogical), "*"),
		wire.FieldsOf(new(*Config), "Script"),
		wire.FieldsOf(new(*EagerConfig), "DLQ", "Sequencer", "Stage", "Staging", "Target"),
		Set,
		chaos.Set,
		decorators.Set,
		diag.New,
		immediate.Set,
		scriptRuntime.Set,
		scriptSequencer.Set,
		sinkprod.Set,
		staging.Set,
		target.Set,
	))
}
