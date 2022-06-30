// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package mylogical contains a command to perform logical replication
// from a mysql source server.
package mylogical

import (
	"net"
	"net/http"

	"github.com/cockroachdb/cdc-sink/internal/source/mylogical"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Command returns the pglogical subcommand.
func Command() *cobra.Command {
	cfg := &mylogical.Config{}
	var metricsAddr string
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "start a mySQL replication feed",
		Use:   "mylogical",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if metricsAddr != "" {
				if err := metricsServer(metricsAddr); err != nil {
					return err
				}
			}

			loop, cancelLoop, err := mylogical.Start(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			// Pause any log.Exit() or log.Fatal() until the server exits.
			log.DeferExitHandler(func() {
				cancelLoop()
				<-loop.Stopped()
			})
			// Wait for shutdown. The main function uses log.Exit()
			// to call the above handler.
			<-cmd.Context().Done()
			return nil
		},
	}
	cfg.Bind(cmd.Flags())
	cmd.Flags().StringVar(&metricsAddr, "metricsAddr", "",
		"a host:port to serve metrics from at /_/varz")
	return cmd
}

// metricsServer starts a trivial prometheus endpoint server which runs
// until the context is canceled.
func metricsServer(bindAddr string) error {
	mux := &http.ServeMux{}
	mux.HandleFunc("/_/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.Handle("/_/varz", promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				EnableOpenMetrics: true,
				ErrorLog:          log.StandardLogger().WithField("promhttp", "true"),
			})))
	mux.Handle("/", http.NotFoundHandler())

	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return errors.WithStack(err)
	}
	srv := &http.Server{
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}
	log.Infof("metrics server bound to %s", l.Addr())
	go srv.Serve(l)
	return nil
}
