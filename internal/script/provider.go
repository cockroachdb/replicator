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

package script

import (
	"context"
	"net/url"
	"runtime"
	"sync"

	"github.com/cockroachdb/field-eng-powertools/workgroup"
	"github.com/cockroachdb/replicator/internal/util/applycfg"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/dop251/goja"
	"github.com/google/uuid"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideLoader,
)

// ProvideLoader is called by Wire to perform the initial script
// loading, parsing, and top-level api handling. This provider
// may return nil if there is no configuration.
func ProvideLoader(
	ctx context.Context, applyConfigs *applycfg.Configs, cfg *Config, diags *diag.Diagnostics,
) (*Loader, error) {
	// We depend on preflight to expand the file path. Ensure that it
	// has been called at least once.
	if err := cfg.Preflight(); err != nil {
		return nil, err
	}

	// Return an empty version if unconfigured.
	if cfg.FS == nil {
		return &Loader{}, nil
	}

	options := cfg.Options
	if options == nil {
		options = NoOptions
	}

	l := &Loader{
		applyConfigs: applyConfigs,
		diags:        diags,
		fs:           cfg.FS,
		options:      options,
		requireCache: make(map[string]goja.Value),
		rt:           goja.New(),
		rtMu:         &sync.RWMutex{},
		sources:      make(map[string]*sourceJS),
		targets:      make(map[string]*targetJS),
		tasks:        workgroup.WithSize(ctx, 2*runtime.GOMAXPROCS(0), 100_000),
	}

	// Use a "goja" tag on struct fields to control name bindings.
	// Also uncapitalize for better style consistency.
	l.rt.SetFieldNameMapper(goja.TagFieldNameMapper("goja", true))

	// Set up top-level namespace.
	global := l.rt.GlobalObject()
	if err := global.Set("__require_cache", l.rt.ToValue(l.requireCache)); err != nil {
		return nil, err
	}
	if err := global.Set("console", console(l.rt)); err != nil {
		return nil, err
	}
	if err := global.Set("require", l.require); err != nil {
		return nil, err
	}

	// Populate an object that represents the API used by scripts.
	apiModule := l.rt.NewObject()
	l.apiModule = apiModule
	l.requireCache["cdc-sink@v1"] = apiModule // Legacy compatibility.
	l.requireCache["replicator@v1"] = apiModule
	if err := apiModule.Set("configureSource", l.configureSource); err != nil {
		return nil, err
	}
	if err := apiModule.Set("configureTable", l.configureTable); err != nil {
		return nil, err
	}
	if err := apiModule.Set("getTX", notInTransaction); err != nil {
		return nil, err
	}
	if err := apiModule.Set("randomUUID", randomUUID); err != nil {
		return nil, err
	}
	if err := apiModule.Set(replicationKeyName, replicationKeyValue); err != nil {
		return nil, err
	}
	if err := apiModule.Set("setOptions", l.setOptions); err != nil {
		return nil, err
	}
	if err := apiModule.Set("standardMerge", l.standardMerge); err != nil {
		return nil, err
	}

	// Load the main script into the runtime.
	main := url.URL{Scheme: "file", Path: cfg.MainPath}
	if _, err := l.require(main.String()); err != nil {
		return nil, err
	}

	return l, nil
}

// randomUUID returns a string containing a random UUID. It is exported
// via the api object.
func randomUUID() string {
	return uuid.New().String()
}
