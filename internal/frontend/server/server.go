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
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/backend/apply"
	"github.com/cockroachdb/cdc-sink/internal/backend/mutation"
	"github.com/cockroachdb/cdc-sink/internal/backend/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/backend/timestamp"
	"github.com/cockroachdb/cdc-sink/internal/frontend/cdc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Various flags.
var (
	BindAddr = flag.String(
		"bindAddr", ":26258", "the network address to bind to")

	ConnectionString = flag.String(
		"conn",
		"postgresql://root@localhost:26257/defaultdb?sslmode=disable",
		"cockroach connection string",
	)

	IgnoreResolved = flag.Bool("ignoreResolved", false,
		"write data to the target databae immediately, without "+
			"waiting for resolved timestamps")
)

// Main is the entry point to the server.
func Main(ctx context.Context) error {
	flag.Parse()

	cfg, err := pgxpool.ParseConfig(*ConnectionString)
	if err != nil {
		return errors.Wrapf(err, "could not parse %q", *ConnectionString)
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
		return errors.Wrap(err, "could not connect to CockroachDB")
	}
	defer pool.Close()

	swapper, err := timestamp.New(ctx, pool, ident.Resolved)
	if err != nil {
		return err
	}

	watchers, cancel := schemawatch.NewWatchers(pool)
	defer cancel()

	appliers, cancel := apply.New(watchers)
	defer cancel()

	mux := &http.ServeMux{}
	mux.HandleFunc("/_/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			log.Printf("health check failed: %v", err)
			http.Error(w, "health check failed", http.StatusInternalServerError)
			return
		}
		http.Error(w, "OK", http.StatusOK)
	})
	mux.Handle("/", &cdc.Handler{
		Appliers:  appliers,
		Immediate: *IgnoreResolved,
		Pool:      pool,
		Stores:    mutation.New(pool, ident.StagingDb),
		Swapper:   swapper,
		Watchers:  watchers,
	})

	l, err := net.Listen("tcp", *BindAddr)
	if err != nil {
		return errors.Wrapf(err, "could not bind to %q", *BindAddr)
	}

	log.Printf("listening on %s", l.Addr())
	srv := &http.Server{
		Handler: h2c.NewHandler(logWrapper(mux), &http2.Server{}),
	}
	go func() {
		<-ctx.Done()
		log.Println("server shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("error during server shutdown: %v", err)
		}
	}()

	err = srv.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
