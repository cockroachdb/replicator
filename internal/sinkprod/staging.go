// Copyright 2024 The Cockroach Authors
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

package sinkprod

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// These are default names for the staging database schema.
var (
	StagingSchemaDefault = ident.MustSchema(ident.New("_replicator"), ident.Public)
	StagingSchemaLegacy  = ident.MustSchema(ident.New("_cdc_sink"), ident.Public)
)

// StagingConfig defines staging-database connection behaviors.
type StagingConfig struct {
	CommonConfig
	// Create the destination schema.
	CreateSchema bool
	// The name of a SQL schema in the staging cluster to store
	// metadata in.
	Schema ident.Schema
}

// Bind adds flags to the set.
func (c *StagingConfig) Bind(f *pflag.FlagSet) {
	c.CommonConfig.bind(f, "staging")
	c.Schema = StagingSchemaDefault
	f.BoolVar(&c.CreateSchema, "stagingCreateSchema", false,
		"automatically create the staging schema if it does not exist")
	f.Var(ident.NewSchemaFlag(&c.Schema), "stagingSchema",
		"a SQL database schema to store metadata in")
}

// Preflight ensures that unset configuration options have sane defaults
// and returns an error if the StagingConfig is missing any fields for
// which a default cannot be provided.
func (c *StagingConfig) Preflight() error {
	if err := c.CommonConfig.preflight("staging", false); err != nil {
		return err
	}
	if c.Schema.Empty() {
		c.Schema = StagingSchemaDefault
	}
	return nil
}

// ProvideStagingDB is called by Wire to retrieve the name of the
// staging SQL DATABASE. If [StagingSchemaDefault] does not exist, but
// [StagingSchemaLegacy] does, then the latter will be returned.
func ProvideStagingDB(
	ctx context.Context, config *StagingConfig, pool *types.StagingPool,
) (ident.StagingSchema, error) {
	// Respect user override.
	if !config.Schema.Empty() && !ident.Equal(config.Schema, StagingSchemaDefault) {
		return ident.StagingSchema(config.Schema), nil
	}

	// Check for existence of legacy schema.
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT datname FROM pg_database WHERE datname = $1)`,
		StagingSchemaLegacy.Idents(nil)[0].Raw(),
	).Scan(&exists); err != nil {
		return ident.StagingSchema{}, errors.WithStack(err)
	}
	if exists {
		return ident.StagingSchema(StagingSchemaLegacy), nil
	}
	return ident.StagingSchema(StagingSchemaDefault), nil
}

// ProvideStagingPool is called by Wire to create a connection pool that
// accesses the staging cluster. The pool will be closed when the
// context is stopped.
func ProvideStagingPool(
	ctx *stopper.Context, config *StagingConfig, diags *diag.Diagnostics, tgtConfig *TargetConfig,
) (*types.StagingPool, error) {
	// Use target endpoint if needed.
	conn := config.Conn
	if conn == "" {
		conn = tgtConfig.Conn
	}

	ret, err := stdpool.OpenPgxAsStaging(ctx,
		conn,
		stdpool.WithConnectionLifetime(config.MaxLifetime, config.IdleTime, config.JitterTime),
		stdpool.WithDiagnostics(diags, "staging"),
		stdpool.WithMetrics("staging"),
		stdpool.WithPoolSize(config.MaxPoolSize),
		stdpool.WithTransactionTimeout(time.Minute), // Staging shouldn't take that much time.
	)
	if err != nil {
		return nil, err
	}
	ctx.Defer(ret.Close)

	// This sanity-checks the configured schema against the product. For
	// Cockroach and Postgresql, we'll add any missing "public" schema
	// names.
	sch, err := ret.Product.ExpandSchema(config.Schema)
	if err != nil {
		return nil, err
	}
	config.Schema = sch

	if config.CreateSchema {
		if _, err := ret.Exec(ctx, fmt.Sprintf(
			"CREATE DATABASE IF NOT EXISTS %s",
			config.Schema.Idents(nil)[0])); err != nil {
			return nil, err
		}

		if _, err := ret.Exec(ctx, fmt.Sprintf(
			"CREATE SCHEMA IF NOT EXISTS %s",
			config.Schema)); err != nil {
			return nil, err
		}
	}

	return ret, err
}
