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

package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptoRand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideAuthenticator,
	ProvideListener,
	ProvideMux,
	ProvideServer,
	ProvideTLSConfig,
)

// ProvideAuthenticator is called by Wire to construct a JWT-based
// authenticator, or a no-op authenticator if Config.DisableAuth has
// been set.
func ProvideAuthenticator(
	ctx context.Context,
	diags *diag.Diagnostics,
	config *Config,
	pool *types.StagingPool,
	stagingDB ident.StagingSchema,
) (types.Authenticator, func(), error) {
	var auth types.Authenticator
	var cancel func()
	var err error
	if config.DisableAuth {
		log.Info("authentication disabled, any caller may write to the target database")
		auth = trust.New()
		cancel = func() {}
	} else {
		auth, cancel, err = jwt.ProvideAuth(ctx, pool, stagingDB)
	}
	if d, ok := auth.(diag.Diagnostic); ok {
		if err := diags.Register("auth", d); err != nil {
			cancel()
			return nil, nil, err
		}
	}
	return auth, cancel, err
}

// ProvideListener is called by Wire to construct the incoming network
// socket for the server.
func ProvideListener(config *Config, diags *diag.Diagnostics) (net.Listener, func(), error) {
	// Start listening only when everything else is ready.
	l, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not bind to %q", config.BindAddr)
	}
	log.WithField("address", l.Addr()).Info("Server listening")
	if err := diags.Register("listener", diag.DiagnosticFn(func(context.Context) any {
		return l.Addr().String()
	})); err != nil {
		_ = l.Close()
		return nil, nil, err
	}
	return l, func() { _ = l.Close() }, nil
}

// ProvideMux is called by Wire to construct the http.ServeMux that
// routes requests.
func ProvideMux(
	handler *cdc.Handler, stagingPool *types.StagingPool, targetPool *types.TargetPool,
) *http.ServeMux {
	mux := &http.ServeMux{}
	mux.HandleFunc("/_/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := stagingPool.Ping(r.Context()); err != nil {
			log.WithError(err).Warn("health check failed for staging pool")
			http.Error(w, "health check failed for staging", http.StatusInternalServerError)
			return
		}
		if err := targetPool.PingContext(r.Context()); err != nil {
			log.WithError(err).Warn("health check failed for target pool")
			http.Error(w, "health check failed for target", http.StatusInternalServerError)
			return
		}
		http.Error(w, "OK", http.StatusOK)
	})
	mux.Handle("/", logWrapper(handler))
	return mux
}

// ProvideServer is called by Wire to construct the top-level network
// server. This provider will execute the server on a background
// goroutine and will gracefully drain the server when the cancel
// function is called.
func ProvideServer(
	auth types.Authenticator,
	diags *diag.Diagnostics,
	listener net.Listener,
	mux *http.ServeMux,
	tlsConfig *tls.Config,
) (*Server, func()) {
	srv := &http.Server{
		Handler:   h2c.NewHandler(mux, &http2.Server{}),
		TLSConfig: tlsConfig,
	}

	ch := make(chan struct{})
	go func() {
		defer close(ch)

		var err error
		if srv.TLSConfig != nil {
			err = srv.ServeTLS(listener, "", "")
		} else {
			err = srv.Serve(listener)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		log.WithError(err).Error("unable to serve requests")
	}()

	return &Server{auth, diags, mux, ch}, func() {
		log.Info("Server shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.WithError(err).Error("did not shut down cleanly")
		}
		log.Info("Server shutdown complete")
	}
}

// ProvideTLSConfig is called by Wire to load the certificate and key
// from disk, to generate a self-signed localhost certificate, or to
// return nil if TLS has been disabled.
func ProvideTLSConfig(config *Config) (*tls.Config, error) {
	if config.TLSCertFile != "" && config.TLSPrivateKey != "" {
		cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSPrivateKey)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}

	if !config.GenerateSelfSigned {
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
