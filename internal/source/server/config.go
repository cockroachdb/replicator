// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config contains the user-visible configuration for running a CDC
// changefeed server.
type Config struct {
	CDC cdc.Config

	BindAddr           string
	DisableAuth        bool
	GenerateSelfSigned bool
	TLSCertFile        string
	TLSPrivateKey      string
}

var _ logical.Config = (*Config)(nil)

// Base implements logical.Config.
func (c *Config) Base() *logical.BaseConfig {
	return c.CDC.Base()
}

// Bind registers flags.
func (c *Config) Bind(flags *pflag.FlagSet) {
	c.CDC.Bind(flags)

	flags.StringVar(
		&c.BindAddr,
		"bindAddr",
		":26258",
		"the network address to bind to")
	flags.BoolVar(
		&c.DisableAuth,
		"disableAuthentication",
		false,
		"disable authentication of incoming cdc-sink requests; not recommended for production.")
	flags.BoolVar(
		&c.GenerateSelfSigned,
		"tlsSelfSigned",
		false,
		"if true, generate a self-signed TLS certificate valid for 'localhost'")
	flags.StringVar(
		&c.TLSCertFile,
		"tlsCertificate",
		"",
		"a path to a PEM-encoded TLS certificate chain")
	flags.StringVar(
		&c.TLSPrivateKey,
		"tlsPrivateKey",
		"",
		"a path to a PEM-encoded TLS private key")
}

// Preflight implements logical.Config.
func (c *Config) Preflight() error {
	if err := c.CDC.Preflight(); err != nil {
		return err
	}

	if c.BindAddr == "" {
		return errors.New("bindAddr unset")
	}
	if (c.TLSCertFile == "") != (c.TLSPrivateKey == "") {
		return errors.New("either both of tlsCertificate and tlsPrivateKey must be set, or none")
	}
	if c.GenerateSelfSigned && c.TLSCertFile != "" {
		return errors.New("self-signed certificate requested, but also specified a TLS certificate")
	}

	return nil
}
