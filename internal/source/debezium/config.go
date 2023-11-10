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

package debezium

import (
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdserver"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config contains the user-visible configuration for running a
// debezium server.
type Config struct {
	logical.BaseConfig
	HTTP         stdserver.Config
	TargetSchema ident.Schema
}

var _ logical.Config = (*Config)(nil)

// Base implements logical.Config.
func (c *Config) Base() *logical.BaseConfig {
	return &c.BaseConfig
}

// Bind registers flags.
func (c *Config) Bind(flags *pflag.FlagSet) {
	c.BaseConfig.Bind(flags)
	c.HTTP.Bind(flags)
	flags.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	if err := c.BaseConfig.Preflight(); err != nil {
		return err
	}
	if err := c.HTTP.Preflight(); err != nil {
		return err
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target database specified")
	}
	return nil
}
