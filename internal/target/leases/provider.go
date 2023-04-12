// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package leases

import (
	"context"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideLeases,
)

// ProvideLeases is called by Wire to configure the work-leasing strategy.
//
// This can be removed once stagingDB once SELECT FOR UPDATE uses
// replicated locks.
//
// https://github.com/cockroachdb/cockroach/issues/100194
func ProvideLeases(
	ctx context.Context, pool *pgxpool.Pool, stagingDB ident.StagingDB,
) (types.Leases, error) {
	return New(ctx, Config{
		Guard:      time.Second,
		Lifetime:   5 * time.Second,
		RetryDelay: time.Second,
		Poll:       time.Second,
		Pool:       pool,
		Target:     ident.NewTable(stagingDB.Ident(), ident.Public, ident.New("leases")),
	})
}
