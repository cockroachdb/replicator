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
	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
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
	ProvideBaseConfig,
	ProvideStagingDB,
	ProvideStagingPool,
	ProvideTargetPool,
	ProvideUserScriptConfig,
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
	applyConfigs *applycfg.Configs,
	baseConfig *BaseConfig,
	memo types.Memo,
	scriptLoader *script.Loader,
	stagingPool *types.StagingPool,
	targetPool *types.TargetPool,
	watchers types.Watchers,
	versionCheck *version.Checker,
) (*Factory, error) {
	warnings, err := versionCheck.Check(ctx)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		for _, w := range warnings {
			log.Warn(w)
		}
		return nil, errors.New("manual schema change required")
	}

	return &Factory{
		appliers:     appliers,
		applyConfigs: applyConfigs,
		baseConfig:   baseConfig,
		memo:         memo,
		scriptLoader: scriptLoader,
		stagingPool:  stagingPool,
		targetPool:   targetPool,
		watchers:     watchers,
	}, nil
}

// ProvideStagingDB is called by Wire to retrieve the name of the
// _cdc_sink SQL DATABASE.
func ProvideStagingDB(config *BaseConfig) (ident.StagingSchema, error) {
	return ident.StagingSchema(config.StagingSchema), nil
}

// ProvideStagingPool is called by Wire to create a connection pool that
// accesses the staging cluster. The pool will be closed by the cancel
// function.
func ProvideStagingPool(
	ctx context.Context, config *BaseConfig,
) (*types.StagingPool, func(), error) {
	ret, cancel, err := stdpool.OpenPgxAsStaging(ctx,
		config.StagingConn,
		stdpool.WithConnectionLifetime(5*time.Minute),
		stdpool.WithMetrics("staging"),
		stdpool.WithPoolSize(128),
		stdpool.WithTransactionTimeout(time.Minute), // Staging shouldn't take that much time.
	)
	if err != nil {
		return nil, nil, err
	}

	// This sanity-checks the configured schema against the product. For
	// Cockroach and Postgresql, we'll add any missing "public" schema
	// names.
	sch, err := ret.Product.ExpandSchema(config.StagingSchema)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	config.StagingSchema = sch

	return ret, cancel, err
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
	ret, cancel, err := stdpool.OpenTarget(ctx, config.TargetConn, options...)
	if err != nil {
		return nil, nil, err
	}

	return ret, cancel, err
}

// ProvideUserScriptConfig is called by Wire to extract the user-script
// configuration.
func ProvideUserScriptConfig(config Config) (*script.Config, error) {
	ret := &config.Base().ScriptConfig
	return ret, ret.Preflight()
}
