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

package seqtest

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	userScript "github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/retire"
	"github.com/cockroachdb/replicator/internal/sequencer/switcher"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/google/wire"
)

func NewSequencerFixture(*all.Fixture, *sequencer.Config, *userScript.Config) (*Fixture, error) {
	panic(wire.Build(
		wire.FieldsOf(new(*base.Fixture), "Context", "StagingDB", "StagingPool", "TargetPool"),
		wire.Bind(new(context.Context), new(*stopper.Context)),

		wire.FieldsOf(new(*all.Fixture),
			"Configs", "Diagnostics", "Fixture", "Stagers", "Watchers"),

		retire.Set,
		switcher.Set,
		userScript.Set,

		wire.Struct(new(Fixture), "*"),
		provideLeases,
	))
}
