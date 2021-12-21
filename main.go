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
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"

	"github.com/cockroachdb/cdc-sink/internal/source/server"
)

var (
	// buildVersion is set by the go linker at build time
	buildVersion = "<unknown>"
	printVersion = flag.Bool("version", false, "print version and exit")
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// First, parse the config.
	flag.Parse()

	if *printVersion {
		fmt.Println("cdc-sink", buildVersion)
		fmt.Println(runtime.Version(), runtime.GOARCH, runtime.GOOS)
		fmt.Println()
		if bi, ok := debug.ReadBuildInfo(); ok {
			fmt.Println(bi.Main.Path, bi.Main.Version)
			for _, m := range bi.Deps {
				for m.Replace != nil {
					m = m.Replace
				}
				fmt.Println(m.Path, m.Version)
			}
		}
		return
	}

	if err := server.Main(ctx); err != nil {
		log.Printf("server exited: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}
