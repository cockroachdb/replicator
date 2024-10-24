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

package mylogical

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"os"
	"strconv"

	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/staging/stage"
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
// replication connection. ServerID and SourceConn are mandatory.
type Config struct {
	DLQ       dlq.Config
	Script    script.Config
	Sequencer sequencer.Config
	Stage     stage.Config           // Staging table configuration.
	Staging   sinkprod.StagingConfig // Staging database configuration.
	Target    sinkprod.TargetConfig

	InitialGTID   string
	FetchMetadata bool
	SourceConn    string // Connection string for the source db.
	ProcessID     uint32 // A unique ID to identify this process to the master.
	// The SQL schema in the target cluster to write into. This value is
	// optional if a userscript dispatch function is present.
	TargetSchema ident.Schema

	// The fields below are extracted by Preflight.

	host      string
	password  string
	port      uint16
	tlsConfig *tls.Config
	user      string
}

// Bind adds flags to the set. It delegates to the embedded Config.Bind.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQ.Bind(f)
	c.Script.Bind(f)
	c.Sequencer.Bind(f)
	c.Stage.Bind(f)
	c.Staging.Bind(f)
	c.Target.Bind(f)

	f.StringVar(&c.InitialGTID, "defaultGTIDSet", "",
		"default GTIDSet. Used if no state is persisted")

	f.Uint32Var(&c.ProcessID, "replicationProcessID", 10,
		"the replication process id to report to the source database")
	f.StringVar(&c.SourceConn, "sourceConn", "",
		"the source database's connection string")
	f.BoolVar(&c.FetchMetadata, "fetchMetadata", false,
		"fetch column metadata explicitly, for older version of MySQL that don't support binlog_row_metadata")
	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")

	// Set to true below.
	if err := f.MarkHidden(sequencer.AssumeIdempotent); err != nil {
		panic(err)
	}
}

func newClientTLSConfig(
	params url.Values, insecureSkipVerify bool, serverName string,
) (*tls.Config, error) {
	caPem, err := os.ReadFile(params.Get("sslrootcert"))
	if err != nil {
		return nil, err
	}
	var certPem, keyPem []byte
	if params.Get("sslcert") != "" {
		certPem, err = os.ReadFile(params.Get("sslcert"))
		if err != nil {
			return nil, err
		}
		keyPem, err = os.ReadFile(params.Get("sslkey"))
		if err != nil {
			return nil, err
		}
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, errors.New("failed to add ca PEM")
	}
	var certs []tls.Certificate
	if certPem != nil {
		cert, err := tls.X509KeyPair(certPem, keyPem)
		if err != nil {
			return nil, errors.New("failed to add ca PEM")
		}
		certs = []tls.Certificate{cert}
	}
	config := &tls.Config{
		Certificates:       certs,
		InsecureSkipVerify: insecureSkipVerify,
		RootCAs:            pool,
		ServerName:         serverName,
	}
	return config, nil
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
	if err := c.Stage.Preflight(); err != nil {
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

	if c.TargetSchema.Empty() {
		return errors.New("no target schema specified")
	}

	if c.SourceConn == "" {
		return errors.New("no SourceConn was configured")
	}

	u, err := url.Parse(c.SourceConn)
	if err != nil {
		return err
	}
	port, err := strconv.ParseInt(u.Port(), 0, 16)
	if err != nil {
		return errors.Wrapf(err, "invalid port %s", u.Port())
	}
	c.host = u.Hostname()
	c.port = uint16(port)
	c.user = u.User.Username()
	c.password, _ = u.User.Password()
	params := u.Query()
	sslmode := params.Get("sslmode")

	switch sslmode {
	case "disable":
		// tls configuration won't be set if we disable sslmode
	case "require", "verify-ca", "verify-full":
		c.tlsConfig, err = newClientTLSConfig(params, sslmode == "require", u.Hostname())
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("invalid sslmode: %q", sslmode)
	}
	return nil
}
