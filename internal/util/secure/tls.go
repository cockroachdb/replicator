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

// Package secure provides utilities to configure secure transport.
package secure

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptoRand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/pkg/errors"
)

// GetCA retrieves the pool of CA certificates
// from the system and the specified file.
func GetCA(path string) (*x509.CertPool, error) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.New("failed to get system certificates")
	}
	if path == "" {
		return pool, nil
	}
	caPem, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read CA certificate")
	}
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, errors.New("failed to add CA")
	}
	return pool, nil
}

// ParseTLSOptions returns a slice of TLS configuration to try, based on the
// sslmode specified in the connection URL
// The returned slice must contain at least one element.
// A nil element indicates a no-SSL configuration.
// It allows a uniform way to specify connections to various backends
// following the Postgres connection specifications.
func ParseTLSOptions(url *url.URL) ([]*tls.Config, error) {
	params := url.Query()
	if !params.Has("sslmode") {
		return []*tls.Config{nil}, nil
	}
	// Mimicking semantics of the Postgres URL connection
	sslmode := params.Get("sslmode")
	switch sslmode {
	case "disable":
		return []*tls.Config{nil}, nil
	case "allow":
		// try insecure first
		return []*tls.Config{nil, {InsecureSkipVerify: true}}, nil
	case "prefer":
		// try secure first
		return []*tls.Config{{InsecureSkipVerify: true}, nil}, nil
	case "require":
		// only secure
		if !params.Has("sslrootcert") {
			return []*tls.Config{{InsecureSkipVerify: true}}, nil
		}
		// According to PostgreSQL documentation, if a root CA file exists,
		// the behavior of sslmode=require should be the same as that of verify-ca
		fallthrough
	case "verify-ca":
		pool, err := GetCA(params.Get("sslrootcert"))
		if err != nil {
			return nil, errors.Wrap(err, "unable to read CA certificate")
		}
		// Adding CA, so certificate from the server can be verified.
		// InsecureSkipVerify is true, because we are not checking
		// if the server host name matches the certificate.
		// We are supplying our own function to verify the certificate.
		return []*tls.Config{
			{
				InsecureSkipVerify:    true,
				RootCAs:               pool,
				VerifyPeerCertificate: makeVerifyPeerCertificate(pool),
			},
		}, nil
	case "verify-full":
		pool, err := GetCA(params.Get("sslrootcert"))
		if err != nil {
			return nil, errors.Wrap(err, "unable to read CA certificate")
		}
		// Get the client key pair, if available, for optional
		// certificate base authentication.
		certs, err := getKeyPair(params.Get("sslcert"), params.Get("sslkey"))
		if err != nil {
			return nil, errors.Wrap(err, "unable to read key pair")
		}
		// Verifying that certificate is trusted and the host matches.
		// Optionally, use client certificate authentication, if provided.
		return []*tls.Config{{
			Certificates: certs,
			RootCAs:      pool,
			ServerName:   url.Hostname(),
		}}, nil
	default:
		return nil, fmt.Errorf("sslmode %q is invalid", sslmode)
	}
}

// TLSConfig loads the certificate and key
// from disk, to generate a self-signed localhost certificate, or to
// return nil if TLS has been disabled.
func TLSConfig(certFile string, privateKey string, generateSelfSigned bool) (*tls.Config, error) {
	if certFile != "" && privateKey != "" {
		cert, err := tls.LoadX509KeyPair(certFile, privateKey)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}

	if !generateSelfSigned {
		return nil, nil
	}

	// Loosely based on https://golang.org/src/crypto/tls/generate_cert.go
	priv, err := ecdsa.GenerateKey(elliptic.P256(), cryptoRand.Reader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate private key")
	}

	now := time.Now().UTC()

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := cryptoRand.Int(cryptoRand.Reader, serialNumberLimit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate serial number")
	}

	cert := x509.Certificate{
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		NotBefore:             now,
		NotAfter:              now.AddDate(1, 0, 0),
		SerialNumber:          serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Cockroach Labs"},
		},
	}

	bytes, err := x509.CreateCertificate(cryptoRand.Reader, &cert, &cert, &priv.PublicKey, priv)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate certificate")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{bytes},
			PrivateKey:  priv,
		}}}, nil
}

// getKeyPair retrieves the key pair from the files specified.
// If both certPath and keyPath are empty, we won't return
// any key pair, as the client won't be doing certificate based authentication.
func getKeyPair(certPath string, keyPath string) ([]tls.Certificate, error) {
	if certPath == "" {
		// if certPath is empty, the keyPath should be empty as well.
		if keyPath == "" {
			return nil, nil
		}
		return nil, errors.New("private key provided without certificate")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	return []tls.Certificate{cert}, nil
}

// Code from https://github.com/jackc/pgx/blob/6f7400f4282d174f272c9548c21dd9dd75fc4bec/pgconn/config.go#L656
func makeVerifyPeerCertificate(
	rootCAs *x509.CertPool,
) func(certificates [][]byte, _ [][]*x509.Certificate) error {
	return func(certificates [][]byte, _ [][]*x509.Certificate) error {
		certs := make([]*x509.Certificate, len(certificates))
		for i, asn1Data := range certificates {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				return errors.Wrap(err, "failed to parse certificate")
			}
			certs[i] = cert
		}
		// Leave DNSName empty to skip hostname verification.
		// This will be only used with "verify-ca".
		opts := x509.VerifyOptions{
			Roots:         rootCAs,
			Intermediates: x509.NewCertPool(),
		}
		// Skip the first cert because it's the leaf. All others
		// are intermediates.
		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := certs[0].Verify(opts)
		return err
	}
}
