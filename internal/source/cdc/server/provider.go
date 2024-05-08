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

// Package server contains an HTTP server that installs
// the CDC listener.
package server

import (
	"crypto/tls"
	"net"
	"net/http"

	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/source/cdc"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/secure"
	"github.com/cockroachdb/replicator/internal/util/stdserver"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideAuthenticator,
	ProvideEagerConfig,
	ProvideListener,
	ProvideMux,
	ProvideServer,
	ProvideTLSConfig,
)

// ProvideAuthenticator is called by Wire to construct a JWT-based
// authenticator, or a no-op authenticator if Config.DisableAuth has
// been set.
func ProvideAuthenticator(
	ctx *stopper.Context,
	diags *diag.Diagnostics,
	config *Config,
	pool *types.StagingPool,
	stagingDB ident.StagingSchema,
) (types.Authenticator, error) {
	return stdserver.Authenticator(ctx, diags, &config.HTTP, pool, stagingDB)
}

// ProvideEagerConfig makes the configuration objects depend upon the
// script loader and preflights the configuration.
func ProvideEagerConfig(cfg *Config, _ *script.Loader) (*EagerConfig, error) {
	return (*EagerConfig)(cfg), cfg.Preflight()
}

// ProvideListener is called by Wire to construct the incoming network
// socket for the server.
func ProvideListener(
	ctx *stopper.Context, config *Config, diags *diag.Diagnostics,
) (net.Listener, error) {
	return stdserver.Listener(ctx, &config.HTTP, diags)
}

// ProvideMux is called by Wire to construct the http.ServeMux that
// routes requests.
func ProvideMux(
	handler *cdc.Handler, stagingPool *types.StagingPool, targetPool *types.TargetPool,
) *http.ServeMux {
	return stdserver.Mux(handler, stagingPool, targetPool)
}

// ProvideServer is called by Wire to construct the top-level network
// server. This provider will execute the server on a background
// goroutine and will gracefully drain the server when the cancel
// function is called.
func ProvideServer(
	ctx *stopper.Context,
	auth types.Authenticator,
	diags *diag.Diagnostics,
	listener net.Listener,
	mux *http.ServeMux,
	tlsConfig *tls.Config,
) *stdserver.Server {
	return stdserver.New(ctx, auth, diags, listener, mux, tlsConfig)
}

// ProvideTLSConfig is called by Wire to load the certificate and key
// from disk, to generate a self-signed localhost certificate, or to
// return nil if TLS has been disabled.
func ProvideTLSConfig(config *Config) (*tls.Config, error) {
	return secure.TLSConfig(config.HTTP.TLSCertFile, config.HTTP.TLSPrivateKey, config.HTTP.GenerateSelfSigned)
}
