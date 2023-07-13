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

package logical

import (
	"context"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/staging/version"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideFactory,
	ProvideLoop,
	ProvideBaseConfig,
	ProvideStagingDB,
	ProvideStagingPool,
	ProvideTargetPool,
	ProvideUserScriptConfig,
	ProvideUserScriptTarget,
)

// ProvideBaseConfig is called by wire to extract the BaseConfig from
// the dialect-specific Config.
//
// The script.Loader is referenced here so that any side effects that
// it has on the config, e.g., api.setOptions(), will be evaluated.
func ProvideBaseConfig(config Config, _ *script.Loader) (*BaseConfig, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	return config.Base(), nil
}

// ProvideFactory returns a utility which can create multiple logical
// loops.
func ProvideFactory(
	ctx context.Context,
	appliers types.Appliers,
	config Config,
	memo types.Memo,
	stagingPool *types.StagingPool,
	targetPool *types.TargetPool,
	userscript *script.UserScript,
	watchers types.Watchers,
	versionCheck *version.Checker,
) (*Factory, func(), error) {
	warnings, err := versionCheck.Check(ctx)
	if err != nil {
		return nil, nil, err
	}
	if len(warnings) > 0 {
		for _, w := range warnings {
			log.Warn(w)
		}
		return nil, nil, errors.New("manual schema change required")
	}

	f := &Factory{
		appliers:    appliers,
		cfg:         config,
		memo:        memo,
		stagingPool: stagingPool,
		targetPool:  targetPool,
		watchers:    watchers,
		userscript:  userscript,
	}
	f.mu.loops = make(map[string]*Loop)
	return f, f.Close, nil
}

// ProvideLoop is called by Wire to create a singleton replication loop.
func ProvideLoop(ctx context.Context, factory *Factory, dialect Dialect) (*Loop, error) {
	return factory.Get(ctx, dialect)
}

// ProvideStagingDB is called by Wire to retrieve the name of the
// _cdc_sink SQL DATABASE.
func ProvideStagingDB(config *BaseConfig) (ident.StagingDB, error) {
	return ident.StagingDB(config.StagingDB), nil
}

// ProvideStagingPool is called by Wire to create a connection pool that
// accesses the staging cluster. The pool will be closed by the cancel
// function.
func ProvideStagingPool(
	ctx context.Context, config *BaseConfig,
) (*types.StagingPool, func(), error) {
	return stdpool.OpenPgxAsStaging(ctx,
		config.StagingConn,
		stdpool.WithConnectionLifetime(5*time.Minute),
		stdpool.WithMetrics("staging"),
		stdpool.WithPoolSize(128),
		stdpool.WithTransactionTimeout(time.Minute), // Staging shouldn't take that much time.
	)
}

// ProvideTargetPool is called by Wire to create a connection pool that
// accesses the target cluster. The pool will be closed by the cancel
// function.
func ProvideTargetPool(ctx context.Context, config *BaseConfig) (*types.TargetPool, func(), error) {
	options := []stdpool.Option{
		stdpool.WithConnectionLifetime(5 * time.Minute),
		stdpool.WithMetrics("target"),
		stdpool.WithPoolSize(128),
		stdpool.WithTransactionTimeout(time.Minute), // Staging shouldn't take that much time.
	}

	// We want to force our longest transaction time to respect the
	// apply-duration timeout and/or the backfill window. We'll take
	// the shortest non-zero value.
	txTimeout := config.ApplyTimeout
	if config.BackfillWindow > 0 &&
		(txTimeout == 0 || config.BackfillWindow < config.ApplyTimeout) {
		txTimeout = config.BackfillWindow
	}
	if txTimeout != 0 {
		options = append(options, stdpool.WithTransactionTimeout(txTimeout))
	}
	ret, cancel, err := stdpool.OpenTarget(ctx, config.StagingConn, options...)
	if ret.Product != types.ProductCockroachDB {
		cancel()
		return nil, nil, errors.Errorf("only CockroachDB is a supported target at this time; have %s", ret.Product)
	}
	return ret, cancel, err
}

// ProvideUserScriptConfig is called by Wire to extract the user-script
// configuration.
func ProvideUserScriptConfig(config Config) (*script.Config, error) {
	ret := &config.Base().ScriptConfig
	return ret, ret.Preflight()
}

// ProvideUserScriptTarget is called by Wire and returns the public
// schema of the target database.
func ProvideUserScriptTarget(config *BaseConfig) script.TargetSchema {
	return script.TargetSchema(ident.NewSchema(config.TargetDB, ident.Public))
}
