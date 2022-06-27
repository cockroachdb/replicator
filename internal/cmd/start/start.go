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
	"github.com/cockroachdb/cdc-sink/internal/source/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Command returns the command to start the server.
func Command() *cobra.Command {
	var cfg server.Config
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "start the server",
		Use:   "start",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, cancel, err := server.NewServer(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			defer cancel()

			// Pause any log.Exit() or log.Fatal() until the server exits.
			log.DeferExitHandler(func() {
				cancel()
				<-s.Stopped()
			})
			<-s.Stopped()
			return nil
		},
	}
	cfg.Bind(cmd.Flags())

	return cmd
}
