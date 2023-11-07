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
	"github.com/cockroachdb/cdc-sink/internal/source/cdc/server"
	"github.com/cockroachdb/cdc-sink/internal/util/stdlogical"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/spf13/cobra"
)

// Command returns the command to start the server.
func Command() *cobra.Command {
	var cfg server.Config
	return stdlogical.New(&stdlogical.Template{
		Bind:  cfg.Bind,
		Short: "start the server",
		Start: func(ctx *stopper.Context, cmd *cobra.Command) (any, error) {
			return server.NewServer(ctx, &cfg)
		},
		Use: "start",
	})
}
