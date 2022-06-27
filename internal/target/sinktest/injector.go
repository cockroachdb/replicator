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

package sinktest

import "github.com/google/wire"

// NewFixture constructs a self-contained test fixture.
func NewBaseFixture() (*BaseFixture, func(), error) {
	panic(wire.Build(TestSet))
}

// NewFixture constructs a self-contained test fixture for all services
// in the target sub-packages.
func NewFixture() (*Fixture, func(), error) {
	panic(wire.Build(TestSet))
}
