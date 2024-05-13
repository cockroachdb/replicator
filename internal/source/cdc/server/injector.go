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

	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer/retire"
	"github.com/cockroachdb/replicator/internal/sequencer/switcher"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/source/cdc"
	"github.com/cockroachdb/replicator/internal/staging"
	"github.com/cockroachdb/replicator/internal/target"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/google/wire"
)

var completeSet = wire.NewSet(
	Set,
	cdc.Set,
	diag.New,
	retire.Set,
	script.Set,
	sinkprod.Set,
	staging.Set,
	switcher.Set,
	target.Set,
)

func NewServer(ctx *stopper.Context, config *Config) (*Server, error) {
	panic(wire.Build(
		completeSet,
		wire.Bind(new(context.Context), new(*stopper.Context)),
		wire.FieldsOf(new(*Config), "CDC"),
		wire.FieldsOf(new(*EagerConfig), "Staging", "Target"),
		wire.Struct(new(Server), "*"),
	))
}
