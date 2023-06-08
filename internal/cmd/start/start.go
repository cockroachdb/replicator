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
			s, cancel, err := server.NewServer(cmd.Context(), &cfg)
			if err != nil {
				return err
			}
			// Pause any log.Exit() or log.Fatal() until the server exits.
			log.DeferExitHandler(func() {
				cancel()
				<-s.Stopped()
			})
			// Wait for shutdown. The main function uses log.Exit()
			// to call the above handler.
			<-cmd.Context().Done()
			return nil
		},
	}
	cfg.Bind(cmd.Flags())

	return cmd
}
