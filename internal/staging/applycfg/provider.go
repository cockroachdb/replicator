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

package applycfg

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConfigs,
)

// ProvideConfigs constructs a Configs instance, starting a new
// background goroutine to keep it refreshed.
func ProvideConfigs(
	ctx context.Context, pool *types.StagingPool, targetDB ident.StagingSchema,
) (*Configs, func(), error) {
	target := ident.NewTable(targetDB.Schema(), ident.New("apply_config"))

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
