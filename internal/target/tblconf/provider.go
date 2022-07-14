// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tblconf

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConfigs,
)

// ProvideConfigs constructs a Configs instance, starting a new
// background goroutine to keep it refreshed.
func ProvideConfigs(
	ctx context.Context, pool *pgxpool.Pool, targetDB ident.StagingDB,
) (*Configs, func(), error) {
	target := ident.NewTable(targetDB.Ident(), ident.Public, ident.New("column_config"))

	if _, err := pool.Exec(ctx, fmt.Sprintf(schema, target)); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	cfg := &Configs{
		dataChanged: sync.NewCond(&sync.Mutex{}),
		pool:        pool,
	}
	cfg.mu.data = make(map[ident.Table]*Config)
	cfg.sql.delete = fmt.Sprintf(deleteTemplate, target)
	cfg.sql.loadAll = fmt.Sprintf(loadAllTemplate, target)
	cfg.sql.upsert = fmt.Sprintf(upsertTemplate, target)

	// Ensure initial data load is good.
	if err := cfg.Refresh(ctx); err != nil {
		return nil, nil, err
	}

	// Start a background goroutine to refresh data.
	refreshCtx, cancel := context.WithCancel(context.Background())
	go cfg.refreshLoop(refreshCtx)
	// Once the refresh context has stopped, watches won't fire.
	cfg.watchCtx = refreshCtx

	return cfg, cancel, nil
}
