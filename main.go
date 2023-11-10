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

package main

//go:generate go run github.com/google/wire/cmd/wire gen ./...
//go:generate go run github.com/cockroachdb/crlfmt -w -ignore _gen.go .

import (
	"context"
	golog "log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/cmd/debezium"
	"github.com/cockroachdb/cdc-sink/internal/cmd/dumphelp"
	"github.com/cockroachdb/cdc-sink/internal/cmd/dumptemplates"
	"github.com/cockroachdb/cdc-sink/internal/cmd/fslogical"
	"github.com/cockroachdb/cdc-sink/internal/cmd/licenses"
	"github.com/cockroachdb/cdc-sink/internal/cmd/mkjwt"
	"github.com/cockroachdb/cdc-sink/internal/cmd/mylogical"
	"github.com/cockroachdb/cdc-sink/internal/cmd/pglogical"
	"github.com/cockroachdb/cdc-sink/internal/cmd/preflight"
	"github.com/cockroachdb/cdc-sink/internal/cmd/start"
	"github.com/cockroachdb/cdc-sink/internal/cmd/version"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/util/logfmt"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	joonix "github.com/joonix/log"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func main() {
	var gracePeriod time.Duration
	var logFormat, logDestination string
	var verbosity int
	root := &cobra.Command{
		Use:           "cdc-sink",
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Hijack anything that uses the standard go logger, like http.
			pw := log.WithField("golog", true).Writer()
			log.DeferExitHandler(func() { _ = pw.Close() })
			// logrus will provide timestamp info.
			golog.SetFlags(0)
			golog.SetOutput(pw)

			switch verbosity {
			case 0:
			// No-op
			case 1:
				log.SetLevel(log.DebugLevel)
			default:
				log.SetLevel(log.TraceLevel)
			}

			switch logFormat {
			case "fluent":
				log.SetFormatter(logfmt.Wrap(joonix.NewFormatter()))
			case "text":
				log.SetFormatter(logfmt.Wrap(&log.TextFormatter{
					FullTimestamp:   true,
					PadLevelText:    true,
					TimestampFormat: time.Stamp,
				}))
			default:
				return errors.Errorf("unknown log format: %q", logFormat)
			}

			if logDestination != "" {
				f, err := os.OpenFile(logDestination, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.WithError(err).Error("could not open log output file")
					log.Exit(1)
				}
				log.DeferExitHandler(func() { _ = f.Close() })
				log.SetOutput(f)
			}

			return nil
		},
	}
	f := root.PersistentFlags()
	f.DurationVar(&gracePeriod, "gracePeriod", 30*time.Second, "allow background processes to exit")
	f.StringVar(&logFormat, "logFormat", "text", "choose log output format [ fluent, text ]")
	f.StringVar(&logDestination, "logDestination", "", "write logs to a file, instead of stdout")
	f.CountVarP(&verbosity, "verbose", "v", "increase logging verbosity to debug; repeat for trace")

	root.AddCommand(
		debezium.Command(),
		dumphelp.Command(),
		dumptemplates.Command(),
		fslogical.Command(),
		licenses.Command(),
		mkjwt.Command(),
		mylogical.Command(),
		pglogical.Command(),
		preflight.Command(),
		script.HelpCommand(),
		start.Command(),
		version.Command(),
	)

	stop := stopper.WithContext(context.Background())
	// Stop cleanly on interrupt.
	stop.Go(func() error {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer cancel()
		select {
		case <-ctx.Done():
			log.Info("Interrupted")
			stop.Stop(gracePeriod)
		case <-stop.Stopping():
			// Nothing to do.
		}
		return nil
	})
	// Allow log.Exit() or log.Fatal() to trigger shutdown.
	log.DeferExitHandler(func() {
		stop.Stop(gracePeriod)
		<-stop.Done()
	})

	// Commands can unwrap the stopper as needed.
	if err := root.ExecuteContext(stop); err != nil {
		log.WithError(err).Error("exited")
		log.Exit(1)
	}
	log.Exit(0)
}
