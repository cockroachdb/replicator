// Copyright 2024 The Cockroach Authors
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

// Package db2 contains a command to perform logical replication
// from an IBM DB2 server.
package db2

import (
	"github.com/cockroachdb/cdc-sink/internal/source/db2"
	"github.com/cockroachdb/cdc-sink/internal/util/stdlogical"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/spf13/cobra"
)

// Command returns the pglogical subcommand.
func Command() *cobra.Command {
	cfg := &db2.Config{}
	return stdlogical.New(&stdlogical.Template{
		Bind:  cfg.Bind,
		Short: "start a DB2 replication feed",
		Start: func(ctx *stopper.Context, cmd *cobra.Command) (any, error) {
			return db2.Start(ctx, cfg)
		},
		Use: "db2",
	})
}
