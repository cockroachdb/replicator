// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package script

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/dop251/goja"
	"github.com/google/uuid"
	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideLoader,
	ProvideUserScript,
)

// TargetSchema is an injection point for the target database schema in
// use by the enclosing environment. This is used  for resolving table
// names in the script.
type TargetSchema ident.Schema

// AsSchema unwraps the enclosed schema name.
func (t TargetSchema) AsSchema() ident.Schema {
	return ident.Schema(t)
}

// ProvideLoader is called by Wire to perform the initial script
// loading, parsing, and top-level api handling. This provider
// may return nil if there is no configuration.
func ProvideLoader(cfg *Config) (*Loader, error) {
	// Return an empty version if unconfigured.
	if cfg.FS == nil {
		return nil, nil
	}

	options := cfg.Options
	if options == nil {
		options = NoOptions
	}

	l := &Loader{
		fs:           cfg.FS,
		options:      options,
		requireCache: make(map[string]goja.Value),
		rt:           goja.New(),
		sources:      make(map[string]*sourceJS),
		targets:      make(map[string]*targetJS),
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
	l.requireCache["cdc-sink@v1"] = apiModule
	if err := apiModule.Set("configureSource", l.configureSource); err != nil {
		return nil, err
	}
	if err := apiModule.Set("configureTable", l.configureTable); err != nil {
		return nil, err
	}
	if err := apiModule.Set("randomUUID", randomUUID); err != nil {
		return nil, err
	}
	if err := apiModule.Set("setOptions", l.setOptions); err != nil {
		return nil, err
	}

	// Load the main script into the runtime.
	main := url.URL{Scheme: "file", Path: cfg.MainPath}
	if _, err := l.require(main.String()); err != nil {
		return nil, err
	}

	return l, nil
}

// ProvideUserScript is called by wire to bind the UserScript to the
// target database.
func ProvideUserScript(
	ctx context.Context,
	applyConfigs *apply.Configs,
	boot *Loader,
	pool *pgxpool.Pool,
	target TargetSchema,
	watchers types.Watchers,
) (*UserScript, error) {
	if boot == nil {
		// Un-configured case, return a dummy object.
		return &UserScript{
			Sources: map[ident.Ident]*Source{},
			Targets: map[ident.Table]*Target{},
		}, nil
	}

	watcher, err := watchers.Get(ctx, target.AsSchema().Database())
	if err != nil {
		return nil, err
	}

	ret := &UserScript{
		Sources: map[ident.Ident]*Source{},
		Targets: map[ident.Table]*Target{},
		rt:      boot.rt,
		target:  target.AsSchema(),
		watcher: watcher,
	}

	if err := ret.bind(boot); err != nil {
		return nil, err
	}

	err = retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for tbl, tblCfg := range ret.Targets {
			if err := applyConfigs.Store(ctx, tx, tbl, &tblCfg.Config); err != nil {
				return errors.WithStack(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return errors.WithStack(err)
		}
		if _, err := applyConfigs.Refresh(ctx); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	return ret, err
}

// randomUUID returns a string containing a random UUID. It is exported
// via the api object.
func randomUUID() string {
	return uuid.New().String()
}
