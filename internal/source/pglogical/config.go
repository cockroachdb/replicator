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

package pglogical

import (
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config contains the configuration necessary for creating a
// replication connection. All field, other than TestControls, are
// mandatory unless explicitly indicated.
type Config struct {
	logical.BaseConfig
	logical.LoopConfig

	// The name of the publication to attach to.
	Publication string
	// The replication slot to attach to.
	Slot string
	// Connection string for the source db.
	SourceConn string
	// Enable support for toasted columns
	ToastedColumns bool
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.BaseConfig.Bind(f)

	c.LoopConfig.LoopName = "pglogical"
	c.LoopConfig.Bind(f)
	f.StringVar(&c.Slot, "slotName", "cdc_sink", "the replication slot in the source database")
	f.StringVar(&c.SourceConn, "sourceConn", "", "the source database's connection string")
	f.StringVar(&c.Publication, "publicationName", "",
		"the publication within the source database to replicate")
	f.BoolVar(&c.ToastedColumns, "enableToastedColumns", false,
		"Enable support for toasted columns")
}

// Preflight updates the configuration with sane defaults or returns an
// error if there are missing options for which a default cannot be
// provided.
func (c *Config) Preflight() error {
	if err := c.BaseConfig.Preflight(); err != nil {
		return err
	}
	if err := c.LoopConfig.Preflight(); err != nil {
		return err
	}
	if c.Publication == "" {
		return errors.New("no publication name was configured")
	}
	if c.Slot == "" {
		return errors.New("no replication slot name was configured")
	}
	if c.SourceConn == "" {
		return errors.New("no source connection was configured")
	}
	return nil
}
