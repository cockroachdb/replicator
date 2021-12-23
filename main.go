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
	"flag"
	golog "log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/server"
	"github.com/cockroachdb/cdc-sink/internal/util/logfmt"
	joonix "github.com/joonix/log"
	log "github.com/sirupsen/logrus"
)

var (
	// buildVersion is set by the go linker at build time
	buildVersion   = "<unknown>"
	logFormat      = flag.String("logFormat", "text", "choose log output format [ fluent, text ]")
	logDestination = flag.String("logDestination", "", "write logs to a file, instead of stdout")
	printVersion   = flag.Bool("version", false, "print version and exit")
)

func main() {
	// Hijack anything that uses the standard go logger, like http.
	pw := log.WithField("golog", true).Writer()
	log.DeferExitHandler(func() { _ = pw.Close() })
	// logrus will provide timestamp info.
	golog.SetFlags(0)
	golog.SetOutput(pw)

	flag.Parse()

	switch *logFormat {
	case "fluent":
		log.SetFormatter(logfmt.Wrap(joonix.NewFormatter()))
	case "text":
		log.SetFormatter(logfmt.Wrap(&log.TextFormatter{
			FullTimestamp:   true,
			PadLevelText:    true,
			TimestampFormat: time.Stamp,
		}))
	default:
		log.Errorf("unknown log format: %q", *logFormat)
		log.Exit(1)
	}

	if dest := *logDestination; dest != "" {
		f, err := os.OpenFile(dest, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.WithError(err).Error("could not open log output file")
			log.Exit(1)
		}
		log.DeferExitHandler(func() { _ = f.Close() })
		log.SetOutput(f)
	}

	if *printVersion {
		log.WithFields(log.Fields{
			"build":   buildVersion,
			"runtime": runtime.Version(),
			"arch":    runtime.GOARCH,
			"os":      runtime.GOOS,
		}).Info("cdc-sink")

		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, m := range bi.Deps {
				for m.Replace != nil {
					m = m.Replace
				}
				log.WithFields(log.Fields{
					"sum":     m.Sum,
					"version": m.Version,
				}).Info(m.Path)
			}
		}
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	log.DeferExitHandler(cancel)

	if err := server.Main(ctx); err != nil {
		log.WithError(err).Error("server exited")
		log.Exit(1)
	}
	log.Exit(0)
}
