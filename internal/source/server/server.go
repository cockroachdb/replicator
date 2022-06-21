// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package server contains a generic HTTP server that installs
// the CDC listener.
package server

// This file contains code repackaged from main.go

import (
	"context"
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof" // Register pprof debugging endpoints.
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/target/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/target/resolve"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Various flags.
var (
	BindAddr = flag.String(
		"bindAddr", ":26258", "the network address to bind to")

	ConnectionString = flag.String(
		"conn",
		"postgresql://root@localhost:26257/?sslmode=disable",
		"cockroach connection string",
	)

	DisableAuth = flag.Bool(
		"disableAuthentication",
		false,
		"disable authentication of incoming cdc-sink requests; not recommended for production.")
)

// Main is the entry point to the server.
func Main(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s, stopped, err := newServer(ctx, *BindAddr, *ConnectionString)
	if err != nil {
		return err
	}
	// Pause any log.Exit() or log.Fatal() until the server exits.
	log.DeferExitHandler(func() {
		cancel()
		<-stopped
	})
	err = s.serve()
	<-stopped
	return err
}

type server struct {
	authenticator types.Authenticator
	listener      net.Listener
	srv           *http.Server
}

// newServer performs all of the setup work that's likely to fail before
// actually serving network requests.
func newServer(
	ctx context.Context, bindAddr, connectionString string,
) (_ *server, stopped <-chan struct{}, _ error) {
	cfg, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not parse %q", connectionString)
	}
	// Identify traffic.
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET application_name=$1", "cdc-sink")
		return err
	}
	// Ensure connection diversity through long-lived loadbalancers.
	cfg.MaxConnLifetime = 10 * time.Minute
	// Keep one spare connection.
	cfg.MinConns = 1
	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not connect to CockroachDB")
	}

	swapper, cancelTimeKeeper, err := timekeeper.NewTimeKeeper(ctx, pool, cdc.Resolved)
	if err != nil {
		return nil, nil, err
	}

	watchers, cancelWatchers := schemawatch.NewWatchers(pool)
	appliers, cancelAppliers := apply.NewAppliers(watchers)
	stagers := stage.NewStagers(pool, ident.StagingDB)

	resolvers, cancelResolvers, err := resolve.New(ctx, resolve.Config{
		Appliers:   appliers,
		MetaTable:  resolve.Table,
		Pool:       pool,
		Stagers:    stagers,
		Timekeeper: swapper,
		Watchers:   watchers,
	})
	if err != nil {
		return nil, nil, err
	}

	var authenticator types.Authenticator
	var cancelAuth func()
	if *DisableAuth {
		log.Info("authentication disabled, any caller may write to the target database")
		authenticator = trust.New()
		cancelAuth = func() {}
	} else {
		authenticator, cancelAuth, err = jwt.New(ctx, pool, ident.StagingDB)
	}
	if err != nil {
		return nil, nil, err
	}

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
	mux.Handle("/", logWrapper(&cdc.Handler{
		Authenticator: authenticator,
		Appliers:      appliers,
		Pool:          pool,
		Resolvers:     resolvers,
		Stores:        stagers,
	}))

	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not bind to %q", bindAddr)
	}

	log.WithField("address", l.Addr()).Info("server listening")
	srv := &http.Server{
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	if srv.TLSConfig, err = loadTLSConfig(); err != nil {
		return nil, nil, errors.Wrap(err, "could not load TLS configuration")
	}

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		<-ctx.Done()
		log.Info("server shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.WithError(err).Error("did not shut down cleanly")
		}
		_ = l.Close()
		cancelResolvers()
		cancelAuth()
		cancelAppliers()
		cancelWatchers()
		cancelTimeKeeper()
		pool.Close()
		log.Info("server shutdown complete")
	}()

	return &server{authenticator, l, srv}, ch, nil
}

func (s *server) serve() error {
	var err error
	if s.srv.TLSConfig != nil {
		err = s.srv.ServeTLS(s.listener, "", "")
	} else {
		err = s.srv.Serve(s.listener)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
