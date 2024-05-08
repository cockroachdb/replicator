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

// Package stdlogical contains a template for building a standard
// logical-replication CLI command.
package stdlogical

import (
	"context"
	"net"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers.
	"runtime"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/auth/trust"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Since we're installing the pprof handlers, we also want to enable
// profiling for blocking calls and mutex locking. This is a reasonable
// rate that's also used by CockroachDB proper.
func init() {
	runtime.SetBlockProfileRate(1000)
	runtime.SetMutexProfileFraction(1000)
}

// MetricsAddrFlag is a global flag that will start an HTTP server.
const MetricsAddrFlag = "metricsAddr"

// Config is our standard protocol for configuration objects.
type Config interface {
	Bind(set *pflag.FlagSet)
}

// HasAuthenticator allows the object to supply a [types.Authenticator].
type HasAuthenticator interface {
	GetAuthenticator() types.Authenticator
}

// HasDiagnostics allows the object to supply a [diag.Diagnostics].
type HasDiagnostics interface {
	GetDiagnostics() *diag.Diagnostics
}

// HasServeMux allows the object to provide a [http.ServeMux] to bind
// the endpoints to, if the [MetricsAddrFlag] is not set.
type HasServeMux interface {
	GetServeMux() *http.ServeMux
}

// A Template contains the input for [New].
type Template struct {
	// An optional object for CLI flag registration.
	Config Config
	// An optional default value for [MetricsAddrFlag].
	Metrics string
	// Passed to [cobra.Command.Short].
	Short string
	// Start should return an object that implements zero or more of the
	// capability interfaces in this package.
	Start func(ctx *stopper.Context, cmd *cobra.Command) (started any, err error)
	// Passed to [cobra.Command.Use].
	Use string
	// Called once all setup has been completed.
	testCallback func()
}

// New constructs a standard logical-replication command.
func New(t *Template) *cobra.Command {
	var metricsAddr string
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: t.Short,
		Use:   t.Use,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Print build info on startup so we always have a place
			// to start debugging from.
			if bi, ok := debug.ReadBuildInfo(); ok {
				info := make(log.Fields, len(bi.Settings))
				for _, s := range bi.Settings {
					info[s.Key] = s.Value
				}
				log.WithFields(info).Info("cdc-sink starting")
			}

			// Delegate startup. main.go provides a stopper.
			started, err := t.Start(stopper.From(cmd.Context()), cmd)
			if err != nil {
				return err
			}

			var auth types.Authenticator
			if x, ok := started.(HasAuthenticator); ok {
				auth = x.GetAuthenticator()
			} else {
				auth = trust.New()
			}

			// Find or create a Diagnostics instance.
			var diags *diag.Diagnostics
			if x, ok := started.(HasDiagnostics); ok {
				diags = x.GetDiagnostics()
			} else {
				// main.go provides a stopper.
				diags = diag.New(stopper.From(cmd.Context()))
			}

			// Start metrics on a separate port or bind to an existing mux.
			if metricsAddr != "" {
				cancelServer, err := MetricsServer(auth, metricsAddr, diags)
				if err != nil {
					return err
				}
				defer cancelServer()
			} else if x, ok := started.(HasServeMux); ok {
				AddHandlers(auth, x.GetServeMux(), diags)
			}

			if t.testCallback != nil {
				t.testCallback()
			}
			// Wait for shutdown. The main function uses log.Exit()
			// to call the above handler.
			<-cmd.Context().Done()
			return nil
		},
	}
	if t.Config != nil {
		t.Config.Bind(cmd.Flags())
	}
	cmd.Flags().StringVar(&metricsAddr, MetricsAddrFlag, t.Metrics,
		"a host:port on which to serve metrics and diagnostics")
	return cmd
}

// AddHandlers populates the ServeMux with diagnostic endpoints.
func AddHandlers(auth types.Authenticator, mux *http.ServeMux, diags *diag.Diagnostics) {
	// The pprof handlers attach themselves to the system-default mux.
	// The index page also assumes that the handlers are reachable from
	// this specific prefix. It seems unlikely that this would collide
	// with an actual database schema.
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle("/_/diag", diags.Handler(auth))
	mux.Handle("/_/varz", promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				EnableOpenMetrics: true,
				ErrorLog:          log.StandardLogger().WithField("promhttp", "true"),
			})))
	mux.Handle("/_/", http.NotFoundHandler()) // Reserve all under /_/
}

// MetricsServer starts a trivial HTTP server which runs until canceled.
func MetricsServer(
	auth types.Authenticator, bindAddr string, diags *diag.Diagnostics,
) (func(), error) {
	mux := &http.ServeMux{}
	AddHandlers(auth, mux, diags)
	mux.HandleFunc("/_/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.Handle("/", http.NotFoundHandler())

	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	srv := &http.Server{
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}
	log.Infof("metrics server bound to %s", l.Addr())
	go srv.Serve(l)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}
