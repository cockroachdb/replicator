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
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/google/wire"
)

func newScriptFromFixture(*sinktest.Fixture, *Config, TargetSchema) (*UserScript, error) {
	panic(wire.Build(
		Set,
		wire.FieldsOf(new(*sinktest.Fixture), "BaseFixture", "Configs", "Watchers"),
		wire.FieldsOf(new(*sinktest.BaseFixture), "Context", "Pool"),
	))
}
