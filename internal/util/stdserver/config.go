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

package stdserver

import (
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config contains the user-visible configuration for running an http server.
type Config struct {
	BindAddr           string
	DisableAuth        bool
	GenerateSelfSigned bool
	TLSCertFile        string
	TLSPrivateKey      string
}

// Bind registers flags.
func (c *Config) Bind(flags *pflag.FlagSet) {
	flags.StringVar(
		&c.BindAddr,
		"bindAddr",
		":26258",
		"the network address to bind to")
	flags.BoolVar(
		&c.DisableAuth,
		"disableAuthentication",
		false,
		"disable authentication of incoming Replicator requests; not recommended for production.")
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
