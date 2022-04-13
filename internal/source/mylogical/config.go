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
	"io/ioutil"
	"net/url"
	"strconv"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Config contains the configuration necessary for creating a
// replication connection. ServerID and SourceConn are mandatory.
type Config struct {
	logical.Config

	// Connection string for the source db.
	SourceConn string

	binlogSyncerConfig replication.BinlogSyncerConfig

	// Used in testing to inject errors during processing.
	withChaosProb float32
}

func newClientTLSConfig(
	params url.Values, insecureSkipVerify bool, serverName string,
) (*tls.Config, error) {
	caPem, err := ioutil.ReadFile(params.Get("sslrootcert"))
	if err != nil {
		return nil, err
	}
	var certPem, keyPem []byte
	if params.Get("sslcert") != "" {
		certPem, err = ioutil.ReadFile(params.Get("sslcert"))
		if err != nil {
			return nil, err
		}
		keyPem, err = ioutil.ReadFile(params.Get("sslkey"))
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
	if c.ConsistentPointKey == "" {
		return errors.New("no CheckPointKey was configured")
	}
	serverID, err := strconv.ParseInt(c.ConsistentPointKey, 10, 32)
	if err != nil {
		return errors.New("no CheckPointKey was configured")
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
		return err
	}
	pass, _ := u.User.Password()
	params := u.Query()
	sslmode := params.Get("sslmode")
	var tls *tls.Config

	switch sslmode {
	case "disable":
		// tls configuration won't be set if we disable sslmode
	case "require", "verify-ca", "verify-full":
		tls, err = newClientTLSConfig(params, sslmode == "require", u.Hostname())
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("invalid sslmode: %q", sslmode)
	}

	log.Tracef("TLS %+v", tls)
	c.binlogSyncerConfig = replication.BinlogSyncerConfig{
		ServerID:  uint32(serverID),
		Flavor:    mysql.MySQLFlavor,
		Host:      u.Hostname(),
		Port:      uint16(port),
		User:      u.User.Username(),
		Password:  pass,
		TLSConfig: tls,
	}
	return nil
}
