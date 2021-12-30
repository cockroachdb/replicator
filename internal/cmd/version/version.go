// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package version contains a command to print the build's
// bill-of-materials.
package version

import (
	"runtime"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// BuildVersion is set by the go linker at build time
var BuildVersion = "<unknown>"

// Command returns a command to print the build's bill-of-materials.
func Command() *cobra.Command {
	return &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "print the build's bill-of-materials",
		Use:   "version",
		RunE: func(cmd *cobra.Command, args []string) error {
			log.WithFields(log.Fields{
				"build":   BuildVersion,
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
			return nil
		},
	}
}
