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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/mylogical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
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
	var metricsAddr, targetDB string
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
			cfg.TargetDB = ident.New(targetDB)

			_, stopped, err := mylogical.NewConn(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			<-stopped
			return nil
		},
	}
	f := cmd.Flags()
	f.DurationVar(&cfg.ApplyTimeout, "applyTimeout", 30*time.Second,
		"the maximum amount of time to wait for an update to be applied")
	f.BoolVar(&cfg.Immediate, "immediate", false, "apply data without waiting for transaction boundaries")
	f.IntVar(&cfg.BytesInFlight, "bytesInFlight", 10*1024*1024,
		"apply backpressure when amount of in-flight mutation data reaches this limit")
	f.StringVar(&metricsAddr, "metricsAddr", "", "a host:port to serve metrics from at /_/varz")
	f.DurationVar(&cfg.RetryDelay, "retryDelay", 10*time.Second,
		"the amount of time to sleep between replication retries")
	f.StringVar(&cfg.CheckPointSourceID, "sourceId", "", "a unique Id of the source database")
	f.Uint32Var(&cfg.ServerID, "serverId", 0, "a unique id to identify cdc_sink with the MySQL server")
	f.StringVar(&cfg.SourceConn, "sourceConn", "", "the source database's connection string")
	f.StringVar(&cfg.TargetConn, "targetConn", "", "the target cluster's connection string")
	f.StringVar(&targetDB, "targetDB", "", "the SQL database in the target cluster to update")
	f.IntVar(&cfg.TargetDBConns, "targetDBConns", 1024, "the maximum pool size to the target cluster")
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
