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
	"errors"
	"io/ioutil"
	"strconv"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/go-mysql-org/go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"github.com/xo/dburl"
)

// Config contains the configuration necessary for creating a
// replication connection. All field, other than TestControls, are
// mandatory.
type Config struct {
	logical.Config

	// ServerID is the unique ID to identify the client to the MySQL se
	ServerID uint32

	// Connection string for the source db.
	SourceConn string

	binlogSyncerConfig replication.BinlogSyncerConfig

	// Used in testing to inject errors during processing.
	withChaosProb float32
}

func newClientTLSConfig(
	caPem, certPem, keyPem []byte, insecureSkipVerify bool, serverName string,
) (*tls.Config, error) {
	pool := x509.NewCertPool()
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
	if c.CheckPointSourceID == "" {
		return errors.New("no CheckPointKey was configured")
	}
	if c.SourceConn == "" {
		return errors.New("no SourceConn was configured")
	}
	if c.ServerID == 0 {
		return errors.New("no ServerID was configured")
	}
	u, err := dburl.Parse(c.SourceConn)
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
	// TODO add Tlssupport
	var tls *tls.Config

	switch sslmode {
	case "disable":
		tls = nil
	case "require", "verify-ca", "verify-full":
		caCert, err := ioutil.ReadFile(params.Get("sslrootcert"))
		if err != nil {
			return err
		}
		var cert, key []byte
		if params.Get("sslcert") != "" {
			cert, err = ioutil.ReadFile(params.Get("sslcert"))
			if err != nil {
				return err
			}
			key, err = ioutil.ReadFile(params.Get("sslkey"))
			if err != nil {
				return err
			}
		}
		tls, err = newClientTLSConfig(caCert, cert, key, sslmode == "require", u.Hostname())
		if err != nil {
			return err
		}
	default:
		return errors.New("invalid sslmode")
	}

	log.Tracef("TLS %+v", tls)
	cfg := replication.BinlogSyncerConfig{
		ServerID:  c.ServerID,
		Flavor:    "mysql",
		Host:      u.Hostname(),
		Port:      uint16(port),
		User:      u.User.Username(),
		Password:  pass,
		TLSConfig: tls,
	}

	c.binlogSyncerConfig = cfg

	return nil
}
