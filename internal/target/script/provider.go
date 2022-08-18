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

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/dop251/goja"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideUserScript)

// TargetSchema is an injection point for the target database schema in
// use by the enclosing environment. This is used  for resolving table
// names in the script.
type TargetSchema ident.Schema

// AsSchema unwraps the enclosed schema name.
func (t TargetSchema) AsSchema() ident.Schema {
	return ident.Schema(t)
}

// ProvideUserScript is called by Wire.
func ProvideUserScript(
	ctx context.Context,
	cfg *Config,
	applyConfigs *apply.Configs,
	pool *pgxpool.Pool,
	target TargetSchema,
	watchers types.Watchers,
) (*UserScript, error) {
	// Return an empty version if unconfigured.
	if cfg.FS == nil {
		return &UserScript{
			Options: map[string]interface{}{},
			Sources: make(map[ident.Ident]*Source),
			Targets: make(map[ident.Table]*Target),
		}, nil
	}

	schema := target.AsSchema()
	watcher, err := watchers.Get(ctx, schema.Database())
	if err != nil {
		return nil, err
	}

	s := &UserScript{
		Sources: make(map[ident.Ident]*Source),
		Targets: make(map[ident.Table]*Target),

		fs:      cfg.FS,
		modules: make(map[string]goja.Value),
		rt:      goja.New(),
		target:  schema,
		watcher: watcher,
	}

	// Use a "goja" tag on struct fields to control name bindings.
	// Also uncapitalize for better style consistency.
	s.rt.SetFieldNameMapper(goja.TagFieldNameMapper("goja", true))

	// Set up top-level namespace.
	global := s.rt.GlobalObject()
	if err := global.Set("console", console(s.rt)); err != nil {
		return nil, err
	}
	if err := global.Set("require", s.require); err != nil {
		return nil, err
	}

	// Populate an object that represents the API used by scripts.
	apiModule := s.rt.NewObject()
	s.modules["cdc-sink@v1"] = apiModule
	if err := apiModule.Set("configureSource", s.configureSource); err != nil {
		return nil, err
	}
	if err := apiModule.Set("configureTable", s.configureTable); err != nil {
		return nil, err
	}
	if err := apiModule.Set("setOptions", s.setOptions); err != nil {
		return nil, err
	}

	// Load the main script into the runtime.
	if _, err := s.require("file://" + cfg.MainPath); err != nil {
		return nil, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for tbl, tblCfg := range s.Targets {
		if err := applyConfigs.Store(ctx, tx, tbl, &tblCfg.Config); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := applyConfigs.Refresh(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return s, err
}
