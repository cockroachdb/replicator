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
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/target/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/target/resolve"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/google/wire"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	ctx context.Context, pool *pgxpool.Pool, config Config, stagingDB ident.StagingDB,
) (types.Authenticator, func(), error) {
	if config.DisableAuth {
		log.Info("authentication disabled, any caller may write to the target database")
		return trust.New(), func() {}, nil
	}
	return jwt.ProvideAuth(ctx, pool, stagingDB)
}

// ProvideMetaTable is called by Wire to return the name of the
// pending_timestamps table.
func ProvideMetaTable(stagingDB ident.StagingDB) resolve.MetaTable {
	return resolve.MetaTable(
		ident.NewTable(stagingDB.Ident(), ident.Public, ident.New("pending_timestamps")))
}

// ProvideListener is called by Wire to construct the incoming network
// socket for the server.
func ProvideListener(config Config) (net.Listener, func(), error) {
	// Start listening only when everything else is ready.
	l, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not bind to %q", config.BindAddr)
	}
	log.WithField("address", l.Addr()).Info("Server listening")
	return l, func() { _ = l.Close() }, nil
}

// ProvideMux is called by Wire to construct the http.ServeMux that
// routes requests.
func ProvideMux(handler *cdc.Handler, pool *pgxpool.Pool) *http.ServeMux {
	mux := &http.ServeMux{}
	// The pprof handlers attach themselves to the system-default mux.
	// The index page also assumes that the handlers are reachable from
	// this specific prefix. It seems unlikely that this would collide
	// with an actual database schema.
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.HandleFunc("/_/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			log.WithError(err).Warn("health check failed")
			http.Error(w, "health check failed", http.StatusInternalServerError)
			return
		}
		http.Error(w, "OK", http.StatusOK)
	})
	mux.Handle("/_/varz", promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				EnableOpenMetrics: true,
				ErrorLog:          log.StandardLogger().WithField("promhttp", "true"),
			}),
	))
	mux.Handle("/_/", http.NotFoundHandler()) // Reserve all under /_/
	mux.Handle("/", logWrapper(handler))
	return mux
}

// ProvidePool is called by Wire to construct a connection pool to the
// target cluster.
func ProvidePool(ctx context.Context, config Config) (*pgxpool.Pool, func(), error) {
	cfg, err := stdpool.ParseConfig(config.ConnectionString)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not parse %q", config.ConnectionString)
	}
	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	return pool, pool.Close, errors.Wrap(err, "could not connect to CockroachDB")
}

// ProvideStagingDB is called by Wire to return the name of the
// _cdc_sink SQL DATABASE.
func ProvideStagingDB(config Config) ident.StagingDB {
	return ident.StagingDB(ident.New(config.StagingDB))
}

// ProvideTimeTable is called by Wire to return the name of the
// timestamps table used to track the last-known resolved timestamps for
// a schema.
func ProvideTimeTable(stagingDB ident.StagingDB) timekeeper.TargetTable {
	return timekeeper.TargetTable(
		ident.NewTable(stagingDB.Ident(), ident.Public, ident.New("timestamps")))
}

// ProvideServer is called by Wire to construct the top-level network
// server. This provider will execute the server on a background
// goroutine and will gracefully drain the server when the cancel
// function is called.
func ProvideServer(
	listener net.Listener, mux *http.ServeMux, tlsConfig *tls.Config,
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

	return &Server{ch}, func() {
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
func ProvideTLSConfig(config Config) (*tls.Config, error) {
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

func provideTestConfig(dbInfo *sinktest.DBInfo) Config {
	// Prefer the webhook format for current versions of CRDB.
	useWebhook := true
	if strings.Contains(dbInfo.Version(), "v20.2.") || strings.Contains(dbInfo.Version(), "v21.1.") {
		useWebhook = false
	}

	return Config{
		BindAddr: "127.0.0.1:0",
		// ConnectionString unnecessary; injected by sinktest provider instead.
		GenerateSelfSigned: useWebhook,
	}
}
