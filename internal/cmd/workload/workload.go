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

package workload

import (
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/cdc/server"
	"github.com/cockroachdb/replicator/internal/util/workload"
	"github.com/spf13/cobra"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// Command returns the top-level workload command.
func Command() *cobra.Command {
	top := &cobra.Command{
		Short: "demonstration workloads",
		Use:   "workload",
	}

	pc := &cobra.Command{
		Short: "parent-child schema",
		Use:   "pc",
	}
	top.AddCommand(pc)

	pc.AddCommand(pcDemo())
	pc.AddCommand(pcRun())

	return top
}

func pcDemo() *cobra.Command {
	cfg := &clientConfig{}
	serverCfg := &server.Config{}
	var metricsAddr string

	cmd := &cobra.Command{
		Short: "end-to-end demo that creates a target schema and starts a Replicator server",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := stopper.From(cmd.Context())

			svr, err := cfg.newServer(ctx, serverCfg)
			if err != nil {
				return err
			}

			if err := cfg.createTables(ctx, svr.TargetPool); err != nil {
				return err
			}

			if err := cfg.initURL(svr.GetListener()); err != nil {
				return err
			}

			if !serverCfg.HTTP.DisableAuth {
				if err := cfg.generateJWT(ctx, svr); err != nil {
					return err
				}
			}

			runner, err := cfg.newRunner(ctx,
				workload.NewGeneratorBase(cfg.parentTable, cfg.childTable))
			if err != nil {
				return err
			}

			return runner.Run(ctx)
		},
		Use: "demo",
	}
	cfg.Bind(cmd.Flags())
	serverCfg.Bind(cmd.Flags())
	cmd.Flags().StringVar(&metricsAddr, "metricsAddr", "", "start a metrics server")
	must(cmd.Flags().Set("bindAddr", "127.0.0.1:0"))
	must(cmd.Flags().Set("stagingCreateSchema", "true"))
	must(cmd.Flags().Set("tlsSelfSigned", "true"))
	// Overridden above.
	must(cmd.Flags().MarkHidden("token"))
	must(cmd.Flags().MarkHidden("url"))

	return cmd
}

func pcRun() *cobra.Command {
	cfg := &clientConfig{}
	cmd := &cobra.Command{
		Short: "run a basic parent-child workload against an existing Replicator server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfg.Preflight(); err != nil {
				return err
			}
			ctx := stopper.From(cmd.Context())
			r, err := newRunner(ctx, cfg,
				workload.NewGeneratorBase(cfg.parentTable, cfg.childTable))
			if err != nil {
				return err
			}
			return r.Run(ctx)
		},
		Use: "run",
	}
	cfg.Bind(cmd.Flags())
	return cmd
}
