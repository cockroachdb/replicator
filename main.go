// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

//go:generate go run github.com/cockroachdb/crlfmt -w .

import (
	"context"
	golog "log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/cmd/mkjwt"
	"github.com/cockroachdb/cdc-sink/internal/cmd/start"
	"github.com/cockroachdb/cdc-sink/internal/cmd/version"
	joonix "github.com/joonix/log"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func main() {
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
				log.SetFormatter(joonix.NewFormatter())
			case "text":
				log.SetFormatter(&log.TextFormatter{
					FullTimestamp:   true,
					PadLevelText:    true,
					TimestampFormat: time.Stamp,
				})
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
	f.StringVar(&logFormat, "logFormat", "text", "choose log output format [ fluent, text ]")
	f.StringVar(&logDestination, "logDestination", "", "write logs to a file, instead of stdout")
	f.CountVarP(&verbosity, "verbose", "v", "increase logging verbosity to debug; repeat for trace")

	root.AddCommand(
		mkjwt.Command(),
		start.Command(),
		version.Command(),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	log.DeferExitHandler(cancel)

	if err := root.ExecuteContext(ctx); err != nil {
		log.WithError(err).Error("exited")
		log.Exit(1)
	}
	log.Exit(0)
}
