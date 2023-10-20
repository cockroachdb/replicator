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

// Package secure provides utilities to configure secure transport

package secure

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigureTransport verifies that we can extract
// from a postgres-like connection string the various
// options that we can use to set up a connection
// with a database server.
func TestConfigureTransport(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	// Expected CA Pool.
	caPem, err := os.ReadFile("./testdata/ca.crt")
	r.NoError(err)
	pool, err := x509.SystemCertPool()
	r.NoError(err)
	ok := pool.AppendCertsFromPEM(caPem)
	r.True(ok)

	// Expected Client Key Pair.
	certPem, err := os.ReadFile("./testdata/test.crt")
	r.NoError(err)
	keyPem, err := os.ReadFile("./testdata/test.key")
	r.NoError(err)
	cert, err := tls.X509KeyPair(certPem, keyPem)
	r.NoError(err)

	tests := []struct {
		name   string
		url    string
		want   []*tls.Config
		errMsg string
	}{
		{
			name: "none",
			url:  "db://localhost:1000",
			want: []*tls.Config{nil},
		},
		{
			name: "insecure",
			url:  "db://localhost:1000?sslmode=disable",
			want: []*tls.Config{nil},
		},
		{
			name: "allow",
			url:  "db://localhost:1000?sslmode=allow",
			want: []*tls.Config{nil, {InsecureSkipVerify: true}},
		},
		{
			name: "prefer",
			url:  "db://localhost:1000?sslmode=prefer",
			want: []*tls.Config{{InsecureSkipVerify: true}, nil},
		},
		{
			name: "require",
			url:  "db://localhost:1000?sslmode=require",
			want: []*tls.Config{{InsecureSkipVerify: true}},
		},
		{
			name: "require_noca",
			url:  "db://localhost:1000?sslmode=require",
			want: []*tls.Config{{InsecureSkipVerify: true}},
		},
		{
			name: "require_withca",
			url:  "db://localhost:1000?sslmode=require&sslrootcert=./testdata/ca.crt",
			want: []*tls.Config{
				{
					InsecureSkipVerify:    true,
					RootCAs:               pool,
					VerifyPeerCertificate: makeVerifyPeerCertificate(pool),
				},
			},
		},
		{
			name: "verify-ca",
			url:  "db://localhost:1000?sslmode=verify-ca&sslrootcert=./testdata/ca.crt",
			want: []*tls.Config{
				{
					InsecureSkipVerify:    true,
					RootCAs:               pool,
					VerifyPeerCertificate: makeVerifyPeerCertificate(pool),
				},
			},
		},
		{
			name:   "verify-ca-invalid",
			url:    "db://localhost:1000?sslmode=verify-ca&sslrootcert=./testdata/not_there.crt",
			want:   nil,
			errMsg: `no such file or directory`,
		},
		{
			name: "verify-full",
			url:  "db://localhost:1000?sslmode=verify-full&sslrootcert=./testdata/ca.crt&sslcert=./testdata/test.crt&sslkey=./testdata/test.key",
			want: []*tls.Config{
				{
					Certificates:       []tls.Certificate{cert},
					InsecureSkipVerify: false,
					RootCAs:            pool,
					ServerName:         "localhost",
				},
			},
		},
		{
			name:   "verify-full-invalid-key",
			url:    "db://localhost:1000?sslmode=verify-full&sslrootcert=./testdata/ca.crt&sslcert=./testdata/test.crt&sslkey=./testdata/not_there.key",
			want:   nil,
			errMsg: `unable to read private key`,
		},
		{
			name:   "verify-full-invalid-cert",
			url:    "db://localhost:1000?sslmode=verify-full&sslrootcert=./testdata/ca.crt&sslcert=./testdata/not_there.crt&sslkey=./testdata/test.key",
			want:   nil,
			errMsg: `unable to read certificate`,
		},
		{
			name:   "verify-full-missing-key",
			url:    "db://localhost:1000?sslmode=verify-full&sslrootcert=./testdata/ca.crt&sslcert=./testdata/test.crt",
			want:   nil,
			errMsg: `unable to read private key`,
		},
		{
			name:   "invalid-mode",
			url:    "db://localhost:1000?sslmode=dummy",
			want:   nil,
			errMsg: `sslmode "dummy" is invalid`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse(tt.url)
			r.NoError(err)
			results, err := ConfigureTransport(parsed)
			if tt.errMsg != "" {
				a.ErrorContains(err, tt.errMsg)
				return
			}
			r.NoError(err)
			r.Equal(len(tt.want), len(results))
			for i, want := range tt.want {
				got := results[i]
				if want == nil {
					a.Nil(got)
					continue
				}
				a.Equal(want.Certificates, got.Certificates)
				a.Equal(want.InsecureSkipVerify, got.InsecureSkipVerify)
				a.True(want.RootCAs.Equal(got.RootCAs))
				if want.VerifyPeerCertificate != nil {
					a.NotNil(got.VerifyPeerCertificate)
				}
			}
		})
	}
}
