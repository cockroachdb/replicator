// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package start contains the command to start the server.
package start

import (
	"flag"

	"github.com/cockroachdb/cdc-sink/internal/source/server"
	"github.com/spf13/cobra"
)

// Command returns the command to start the server.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "start the server",
		Use:   "start",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return server.Main(cmd.Context())
		},
	}
	// TODO: Move configuration from globals?
	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	return cmd
}
