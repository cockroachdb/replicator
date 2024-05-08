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
	"time"

	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	defaultStandbyTimeout = 5 * time.Second
)

// EagerConfig is a hack to get Wire to move userscript evaluation to
// the beginning of the injector. This allows CLI flags to be set by the
// script.
type EagerConfig Config

// Config contains the configuration necessary for creating a
// replication connection. All field, other than TestControls, are
// mandatory unless explicitly indicated.
type Config struct {
	DLQ       dlq.Config
	Script    script.Config
	Sequencer sequencer.Config
	Staging   sinkprod.StagingConfig
	Target    sinkprod.TargetConfig

	// The name of the publication to attach to.
	Publication string
	// The replication slot to attach to.
	Slot string
	// How ofter to report progress to the source database.
	StandbyTimeout time.Duration
	// Connection string for the source db.
	SourceConn string
	// The SQL schema in the target cluster to write into. This value is
	// optional if a userscript dispatch function is present.
	TargetSchema ident.Schema
	// Enable support for toasted columns
	ToastedColumns bool
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQ.Bind(f)
	c.Script.Bind(f)
	c.Sequencer.Bind(f)
	c.Staging.Bind(f)
	c.Target.Bind(f)

	f.StringVar(&c.Publication, "publicationName", "",
		"the publication within the source database to replicate")
	f.StringVar(&c.Slot, "slotName", "cdc_sink", "the replication slot in the source database")
	f.StringVar(&c.SourceConn, "sourceConn", "", "the source database's connection string")
	f.DurationVar(&c.StandbyTimeout, "standbyTimeout", defaultStandbyTimeout,
		"how often to report WAL progress to the source server")
	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")
	f.BoolVar(&c.ToastedColumns, "enableToastedColumns", false,
		"Enable support for toasted columns")
}

// Preflight updates the configuration with sane defaults or returns an
// error if there are missing options for which a default cannot be
// provided.
func (c *Config) Preflight() error {
	if err := c.DLQ.Preflight(); err != nil {
		return err
	}
	if err := c.Script.Preflight(); err != nil {
		return err
	}
	if err := c.Sequencer.Preflight(); err != nil {
		return err
	}
	if err := c.Staging.Preflight(); err != nil {
		return err
	}
	if err := c.Target.Preflight(); err != nil {
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
	if c.StandbyTimeout == 0 {
		c.StandbyTimeout = defaultStandbyTimeout
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target schema specified")
	}
	return nil
}
