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

// Package dumphelp contains a hidden command to write the help strings
// for all files to an output directory.
package dumphelp

import (
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// Command returns the dumphelp command.
func Command() *cobra.Command {
	return &cobra.Command{
		Use:    "dumphelp",
		Short:  "write command help strings to files",
		Args:   cobra.ExactArgs(1),
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			base := args[0]
			if err := os.MkdirAll(base, 0755); err != nil {
				return err
			}

			for _, toDump := range cmd.Parent().Commands() {
				out, err := os.Create(filepath.Join(base, toDump.Name()+".help.txt"))
				if err != nil {
					return err
				}
				_, err = io.WriteString(out, toDump.UsageString())
				_ = out.Close()
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}
