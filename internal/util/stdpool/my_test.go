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

// Package stdpool creates standardized database connection pools.
package stdpool

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetConnOptions verifies that connection
// options are correctly extracted from the URL and
// the TLS options.
func TestGetConnOptions(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	// CA Pool
	caPem, err := os.ReadFile("./testdata/ca.crt")
	r.NoError(err)
	pool, err := x509.SystemCertPool()
	r.NoError(err)
	ok := pool.AppendCertsFromPEM(caPem)
	r.True(ok)
	// Client certificate
	certPem, err := os.ReadFile("./testdata/test.crt")
	r.NoError(err)
	keyPem, err := os.ReadFile("./testdata/test.key")
	r.NoError(err)
	cert, err := tls.X509KeyPair(certPem, keyPem)
	r.NoError(err)
	tests := []struct {
		name          string
		option        *tls.Config
		tlsConfigName string
		url           string
		wantConn      string
		wantErr       error
	}{
		{
			name:     "insecure",
			url:      "mysql://test:test@localhost:3306/mysql",
			wantConn: "test:test@tcp(localhost:3306)/mysql?sql_mode=ansi",
		},
		{
			name: "skip-verify",
			option: &tls.Config{
				InsecureSkipVerify: true,
			},
			tlsConfigName: tlsConfigNames.newName("mysql_driver"),
			url:           "mysql://test:test@localhost:3306/mysql",
			wantConn:      "test:test@tcp(localhost:3306)/mysql?sql_mode=ansi&tls=",
		},
		{
			name: "full",
			option: &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      pool,
			},
			tlsConfigName: tlsConfigNames.newName("mysql_driver"),
			url:           "mysql://test:test@localhost:3306/mysql",
			wantConn:      "test:test@tcp(localhost:3306)/mysql?sql_mode=ansi&tls=",
		},
	}
	t.Parallel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse(tt.url)
			r.NoError(err)
			gotc, err := getConnString(parsed, tt.tlsConfigName, tt.option)
			if tt.wantErr != nil {
				a.Equal(tt.wantErr, err)
				return
			}
			r.NoError(err)
			a.Equal(tt.wantConn+tt.tlsConfigName, gotc)
		})
	}
}
