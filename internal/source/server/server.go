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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
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
	listener net.Listener
	srv      *http.Server
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

	swapper, err := timekeeper.NewTimeKeeper(ctx, pool, cdc.Resolved)
	if err != nil {
		return nil, nil, err
	}

	watchers, cancelWatchers := schemawatch.NewWatchers(pool)
	appliers, cancelAppliers := apply.NewAppliers(watchers)

	mux := &http.ServeMux{}
	mux.HandleFunc("/_/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			log.WithError(err).Warn("health check failed")
			http.Error(w, "health check failed", http.StatusInternalServerError)
			return
		}
		http.Error(w, "OK", http.StatusOK)
	})
	mux.Handle("/_/", http.NotFoundHandler()) // Reserve all under /_/
	mux.Handle("/", &cdc.Handler{
		Appliers: appliers,
		Pool:     pool,
		Stores:   stage.NewStagers(pool, ident.StagingDB),
		Swapper:  swapper,
		Watchers: watchers,
	})

	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not bind to %q", bindAddr)
	}

	log.WithField("address", l.Addr()).Info("server listening")
	srv := &http.Server{
		Handler: h2c.NewHandler(logWrapper(mux), &http2.Server{}),
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
		cancelAppliers()
		cancelWatchers()
		pool.Close()
		log.Info("server shutdown complete")
	}()

	return &server{l, srv}, ch, nil
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
