// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConfigs,
	ProvideFactory,
)

// ProvideConfigs constructs a Configs instance, starting a new
// background goroutine to keep it refreshed.
func ProvideConfigs(
	ctx context.Context, pool *pgxpool.Pool, targetDB ident.StagingDB,
) (*Configs, func(), error) {
	target := ident.NewTable(targetDB.Ident(), ident.Public, ident.New("apply_config"))

	if _, err := pool.Exec(ctx, fmt.Sprintf(confSchema, target)); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	cfg := &Configs{pool: pool}
	cfg.mu.data = make(map[ident.Table]*Config)
	cfg.mu.updated = make(chan struct{})
	cfg.sql.delete = fmt.Sprintf(deleteConfTemplate, target)
	cfg.sql.loadAll = fmt.Sprintf(loadConfTemplate, target)
	cfg.sql.upsert = fmt.Sprintf(upsertConfTemplate, target)

	// Ensure initial data load is good.
	if _, err := cfg.Refresh(ctx); err != nil {
		return nil, nil, err
	}

	// Start a background goroutine to refresh data.
	refreshCtx, cancel := context.WithCancel(context.Background())
	go cfg.refreshLoop(refreshCtx)
	// Once the refresh context has stopped, watches won't fire.
	cfg.watchCtx = refreshCtx

	return cfg, cancel, nil
}

// ProvideFactory is called by Wire to construct the factory. The cancel
// function will, in turn, destroy the per-schema types.Applier
// instances.
func ProvideFactory(configs *Configs, watchers types.Watchers) (types.Appliers, func()) {
	f := &factory{
		configs:  configs,
		watchers: watchers,
	}
	f.mu.instances = make(map[ident.Table]*apply)
	return f, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, fn := range f.mu.cleanup {
			fn()
		}
		f.mu.cleanup = nil
		f.mu.instances = nil
	}
}
