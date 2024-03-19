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

package secure

import (
	"crypto/tls"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Config stores the TLS parameters passed via command line options.
type Config struct {
	CaCert     string
	ClientCert string
	ClientKey  string
	SkipVerify bool

	built *tls.Config // computed by Preflight
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	// TLS
	f.StringVar(&c.CaCert, "tlsCACertificate", "", "the path of the base64-encoded CA file")
	f.StringVar(&c.ClientCert, "tlsCertificate", "", "the path of the base64-encoded client certificate file")
	f.StringVar(&c.ClientKey, "tlsPrivateKey", "", "the path of the base64-encoded client private key")
	f.BoolVar(&c.SkipVerify, "insecureSkipVerify", false, "If true, disable client-side validation of responses")
}

// Preflight builds a tls.Config using the command line options provided at start up.
func (c *Config) Preflight() error {
	tlsConfig := &tls.Config{}
	enabled := false
	if c.ClientCert != "" || c.ClientKey != "" {
		var err error
		if c.ClientCert == "" {
			return errors.New("tlsCertificate must specified if tlsPrivateKey is present")
		}
		if c.ClientKey == "" {
			return errors.New("tlsPrivateKey must specified if tlsCertificate is present")
		}
		if tlsConfig, err = TLSConfig(c.ClientCert, c.ClientKey, false); err != nil {
			return errors.Wrap(err, "cannot load certificate or key")
		}
		enabled = true
	}
	if c.CaCert != "" {
		caPool, err := GetCA(c.CaCert)
		if err != nil {
			return errors.Wrap(err, "cannot load CA certificate")
		}
		tlsConfig.RootCAs = caPool
		enabled = true
	}
	if c.SkipVerify {
		tlsConfig.InsecureSkipVerify = true
		enabled = true
	}
	// if it is not enabled we leave it as nil the command line options provided at start up.
	if enabled {
		c.built = tlsConfig
	}
	return nil
}

// AsTLSConfig returns the tls.Config object built from
func (c *Config) AsTLSConfig() *tls.Config {
	return c.built
}
