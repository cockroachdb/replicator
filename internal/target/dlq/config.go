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

package dlq

import (
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/spf13/pflag"
)

const defaultTableName = "cdc_sink_dlq"

// Config controls the DLQ behavior.
type Config struct {
	TableName ident.Ident // Default name within the target schema.
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	f.Var(ident.NewValue(defaultTableName, &c.TableName), "dlqTableName",
		"the name of a table in the target schema for storing dead-letter entries")
}

// Preflight validates the configuration.
func (c *Config) Preflight() error {
	if c.TableName.Empty() {
		c.TableName = ident.New(defaultTableName)
	}
	return nil
}
