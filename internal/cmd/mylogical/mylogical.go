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

// Package mylogical contains a command to perform logical replication
// from a mysql source server.
package mylogical

import (
	"github.com/cockroachdb/cdc-sink/internal/source/mylogical"
	"github.com/cockroachdb/cdc-sink/internal/util/stdlogical"
	"github.com/spf13/cobra"
)

// Command returns the pglogical subcommand.
func Command() *cobra.Command {
	cfg := &mylogical.Config{}
	return stdlogical.New(&stdlogical.Template{
		Bind:  cfg.Bind,
		Short: "start a mySQL replication feed",
		Start: func(cmd *cobra.Command) (any, func(), error) {
			return mylogical.Start(cmd.Context(), cfg)
		},
		Use: "mylogical",
	})
}
