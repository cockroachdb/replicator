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

// Package dumptemplates contains a hidden command that will write
// the apply templates to disk. It is for in-the-field debugging use
// only and does not constitute a stable API.
package dumptemplates

import (
	"fmt"

	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/util/fscopy"
	"github.com/spf13/cobra"
)

// Command returns the dumptemplates command.
func Command() *cobra.Command {
	var path string
	cmd := &cobra.Command{
		Args:   cobra.NoArgs,
		Hidden: true,
		Use:    "dumptemplates",
		Short:  "write the query templates to disk (debugging use only)",
		Long: fmt.Sprintf(`This command writes the apply templates to disk.

When running Replicator, set the %[1]s environment variable to the directory
that is passed to the --path flag. The indicated directory must contain
a subdirectory named %[2]s.

replicator dumptemplates --path .
%[1]s=. replicator start ....
`, apply.TemplateOverrideEnv, apply.QueryDir),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fscopy.Copy(apply.EmbeddedTemplates, path)
		},
	}
	cmd.Flags().StringVar(&path, "path", ".",
		"the directory to write the query templates to")
	return cmd
}
