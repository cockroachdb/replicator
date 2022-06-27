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

package server

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/resolve"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/google/wire"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
)

func NewServer(ctx context.Context, config Config) (*Server, func(), error) {
	panic(wire.Build(
		Set,
		apply.Set,
		cdc.Set,
		schemawatch.Set,
		stage.Set,
		resolve.Set,
		timekeeper.Set,
		// Additional bindings to create a production-ready injector.
		ProvidePool,
		ProvideStagingDB,
		ProvideMetaTable,
		ProvideTimeTable,
		wire.Bind(new(pgxtype.Querier), new(*pgxpool.Pool)),
	))
}
