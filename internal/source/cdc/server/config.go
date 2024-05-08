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

package server

import (
	stagingProd "github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/source/cdc"
	"github.com/cockroachdb/replicator/internal/util/stdserver"
	"github.com/spf13/pflag"
)

// EagerConfig is a hack to force Wire to build the script loader first.
// This allows the userscript to modify all CLI options.
type EagerConfig Config

// Config contains the user-visible configuration for running a CDC
// changefeed server.
type Config struct {
	CDC     cdc.Config
	HTTP    stdserver.Config
	Staging stagingProd.StagingConfig
	Target  stagingProd.TargetConfig
}

// Bind registers flags.
func (c *Config) Bind(flags *pflag.FlagSet) {
	c.CDC.Bind(flags)
	c.HTTP.Bind(flags)
	c.Staging.Bind(flags)
	c.Target.Bind(flags)
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	if err := c.CDC.Preflight(); err != nil {
		return err
	}
	if err := c.HTTP.Preflight(); err != nil {
		return err
	}
	if err := c.Staging.Preflight(); err != nil {
		return err
	}
	if err := c.Target.Preflight(); err != nil {
		return err
	}
	return nil
}
