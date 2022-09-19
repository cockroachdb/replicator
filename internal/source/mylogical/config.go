// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mylogical

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"os"
	"strconv"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config contains the configuration necessary for creating a
// replication connection. ServerID and SourceConn are mandatory.
type Config struct {
	logical.Config

	SourceConn string // Connection string for the source db.
	ProcessID  uint32 // A unique ID to identify this process to the master.

	// The fields below are extracted by Preflight.

	host      string
	password  string
	port      uint16
	tlsConfig *tls.Config
	user      string
}

// Bind adds flags to the set. It delegates to the embedded Config.Bind.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.Config.Bind(f)
	f.StringVar(&c.DefaultConsistentPoint, "defaultGTIDSet", "",
		"default GTIDSet. Used if no state is persisted")
	f.Uint32Var(&c.ProcessID, "replicationProcessID", 10,
		"the replication process id to report to the source database")
	f.StringVar(&c.LoopName, "loopName", "mylogical",
		"identify the replication loop in metrics")
	f.StringVar(&c.SourceConn, "sourceConn", "",
		"the source database's connection string")
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
	if err := c.Config.Preflight(); err != nil {
		return err
	}
	if c.LoopName == "" {
		return errors.New("no LoopName was configured")
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
