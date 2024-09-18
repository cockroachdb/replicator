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

package oraclelogminer

import (
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// EagerConfig is a hack to get Wire to move userscript evaluation to
// the beginning of the injector. This allows CLI flags to be set by the
// script.
type EagerConfig Config

// Config contains the configuration necessary for creating a
// replication connection. SourceConn is mandatory.
type Config struct {
	DLQ       dlq.Config
	Script    script.Config
	Sequencer sequencer.Config
	Staging   sinkprod.StagingConfig
	Target    sinkprod.TargetConfig

	// TableUser marks the schema of the table. Note that this is different from the user passed
	// in via the connection string to the source database. To use logMiner, the
	// connection string user must be the sys dba. TableUser here means the
	// owner (equivalent to schema in postgres context) of the tables to
	// monitor.
	TableUser string
	// Connection string for the source db.
	SourceConn string
	// The SQL schema in the target cluster to write into. This value is
	// optional if a userscript dispatch function is present.
	TargetSchema ident.Schema

	// SCN: System Change Number to start listening.
	// Per oracle doc, A system change number (SCN) is a logical, internal time
	// stamp used by Oracle Database. SCNs order events that occur within the
	// database, which is necessary to satisfy the ACID properties of a
	// transaction. Oracle Database uses SCNs to mark the SCN before which all
	// changes are known to be on disk so that recovery avoids applying
	// unnecessary redo. The database also uses SCNs to mark the point at which
	// no redo exists for a set of data so that recovery can stop.
	// https://docs.oracle.com/cd/E11882_01/server.112/e40540/transact.htm#CNCPT039
	SCN string
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQ.Bind(f)
	c.Script.Bind(f)
	c.Sequencer.Bind(f)
	c.Staging.Bind(f)
	c.Target.Bind(f)

	f.StringVar(&c.SourceConn, "sourceConn", "", "the source database's connection string")

	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")

	f.StringVar(&c.SCN, "scn", "", "starting SCN for logMiner")
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

	// We can disable idempotent tracking in the sequencer stack
	// since the logical stream is idempotent.
	c.Sequencer.IdempotentSource = true

	if c.SourceConn == "" {
		return errors.New("no source connection was configured")
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target schema specified")
	}
	return nil
}
