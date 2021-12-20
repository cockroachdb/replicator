// Copyright 2021 The Cockroach Authors.
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
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptoRand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"math/big"
	"net"
	"time"

	"github.com/pkg/errors"
)

// Various flags to control TLS options.
var (
	GenerateSelfSigned = flag.Bool("tlsSelfSigned", false,
		"if true, generate a self-signed TLS certificate valid for 'localhost'")

	TLSCertFile   = flag.String("tlsCertificate", "", "a path to a PEM-encoded TLS certificate chain")
	TLSPrivateKey = flag.String("tlsPrivateKey", "", "a path to a PEM-encoded TLS private key")
)

// loadTLSConfig returns a TLS configuration, or nil if plaintext should
// be used.
func loadTLSConfig() (*tls.Config, error) {
	// Loosely based on https://golang.org/src/crypto/tls/generate_cert.go
	if *GenerateSelfSigned {
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
	if *TLSCertFile != "" && *TLSPrivateKey != "" {
		cert, err := tls.LoadX509KeyPair(*TLSCertFile, *TLSPrivateKey)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}
	return nil, nil
}
