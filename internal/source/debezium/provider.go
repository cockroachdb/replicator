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

package debezium

import (
	"crypto/tls"
	"net"
	"net/http"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdserver"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	wire.Struct(new(Debezium), "*"),
	ProvideHandler,
	ProvideAuthenticator,
	ProvideListener,
	ProvideMux,
	ProvideServer,
	ProvideTLSConfig,
)

// ProvideHandler is called by Wire to construct an http server handler.
func ProvideHandler(
	ctx *stopper.Context,
	config *Config,
	authenticator types.Authenticator,
	loops *logical.Factory,
	_ *script.Loader,
) (*Handler, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	batcher, err := loops.Immediate(config.TargetSchema)
	if err != nil {
		return nil, err
	}
	return &Handler{
		Authenticator: authenticator,
		Batcher:       batcher,
		Config:        config,
		Stop:          ctx,
	}, nil
}

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
	handler *Handler, stagingPool *types.StagingPool, targetPool *types.TargetPool,
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
	return stdserver.TLSConfig(&config.HTTP)
}
