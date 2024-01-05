// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package db2

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sinkprod"
	"github.com/cockroachdb/cdc-sink/internal/target/dlq"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// EagerConfig is a hack to get Wire to move userscript evaluation to
// the beginning of the injector. This allows CLI flags to be set by the
// script.
type EagerConfig Config

// Config contains the configuration necessary for creating a
// replication connection to a DB2 server. SourceConn is mandatory.
type Config struct {
	DLQ       dlq.Config
	Script    script.Config
	Sequencer sequencer.Config
	Staging   sinkprod.StagingConfig
	Target    sinkprod.TargetConfig

	// InitialLsn is the Log Sequence Number used to start the replication
	InitialLSN string
	// PollingInterval defines how often we should check for new data, if idle.
	PollingInterval time.Duration
	// Connection string for the source db. Mandatory.
	SourceConn string
	// The schema we are replicating. If not provided, all the schemas will be replicated.
	SourceSchema ident.Schema

	// The schema for SQL replication in DB2.
	SQLReplicationSchema ident.Schema
	// The SQL schema in the target cluster to write into.
	TargetSchema ident.Schema
	// The fields below are extracted by Preflight.
	host     string
	password string
	port     uint16
	user     string
	database string
}

// defaultSQLReplicationSchema is the schema in DB2 where the staging tables are located
var defaultSQLReplicationSchema = ident.MustSchema(ident.New("ASNCDC"))

// Bind adds flags to the set. It delegates to the embedded Config.Bind.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQ.Bind(f)
	c.Script.Bind(f)
	c.Sequencer.Bind(f)
	c.Staging.Bind(f)
	c.Target.Bind(f)

	f.StringVar(&c.InitialLSN, "defaultLSN", "",
		"default log sequence number. Used if no state is persisted")
	f.StringVar(&c.SourceConn, "sourceConn", "",
		"the source database's connection string")
	f.Var(ident.NewSchemaFlag(&c.SourceSchema), "sourceSchema",
		"the SQL database schema in the source cluster")
	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")
	f.Var(ident.NewSchemaFlag(&c.SQLReplicationSchema), "replicationSchema",
		"the SQL database schema used by the DB2 SQL replication")
	f.DurationVar(&c.PollingInterval, "pollingInterval", time.Second, "how often we check for new mutations")

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
	if c.SourceConn == "" {
		return errors.New("no SourceConn was configured")
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target schema specified")
	}
	if c.SQLReplicationSchema.Empty() {
		c.SQLReplicationSchema = defaultSQLReplicationSchema
	}
	u, err := url.Parse(c.SourceConn)
	if err != nil {
		return err
	}
	port, err := strconv.ParseUint(u.Port(), 0, 16)
	if err != nil {
		return errors.Wrapf(err, "invalid port %s", u.Port())
	}
	c.host = u.Hostname()
	c.port = uint16(port)
	c.user = u.User.Username()
	c.password, _ = u.User.Password()
	c.database, _ = strings.CutPrefix(u.Path, "/")

	return nil
}
