// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build wireinject
// +build wireinject

package script

import (
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/google/wire"
)

func newScriptFromFixture(*all.Fixture, *Config, TargetSchema) (*UserScript, error) {
	panic(wire.Build(
		Set,
		wire.FieldsOf(new(*all.Fixture), "Fixture", "Configs", "Watchers"),
		wire.FieldsOf(new(*base.Fixture), "Context", "Pool"),
	))
}
