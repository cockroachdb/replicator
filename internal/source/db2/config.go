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

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config contains the configuration necessary for creating a
// replication connection to a DB2 server. SourceConn is mandatory.
type Config struct {
	logical.BaseConfig
	logical.LoopConfig

	// Connection string for the source db. Mandatory.
	SourceConn string
	// The schema we are replicating. If not provided, all the schemas will be replicated.
	SourceSchema ident.Schema

	// The fields below are extracted by Preflight.
	host     string
	password string
	port     uint16
	user     string
	database string
}

var schema string

// Bind adds flags to the set. It delegates to the embedded Config.Bind.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.BaseConfig.Bind(f)
	c.LoopConfig.LoopName = "db2"
	c.LoopConfig.Bind(f)
	f.StringVar(&c.SourceConn, "sourceConn", "",
		"the source database's connection string")
	f.StringVar(&schema, "sourceSchema", "", "the source schema")

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
	if c.LoopName == "" {
		return errors.New("no LoopName was configured")
	}
	if schema != "" {
		c.SourceSchema = ident.MustSchema(ident.New(schema))
	}
	if c.SourceConn == "" {
		return errors.New("no SourceConn was configured")
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
