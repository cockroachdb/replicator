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

// Package version contains a command to print the build's
// bill-of-materials.
package version

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Command returns a command to print the build's bill-of-materials.
func Command() *cobra.Command {
	return &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "print the build's bill-of-materials",
		Use:   "version",
		RunE: func(cmd *cobra.Command, args []string) error {
			if bi, ok := debug.ReadBuildInfo(); ok {
				fields := log.Fields{
					"build":   bi.Main.Version,
					"runtime": runtime.Version(),
					"arch":    runtime.GOARCH,
					"os":      runtime.GOOS,
				}
				for _, setting := range bi.Settings {
					fields[setting.Key] = setting.Value
				}
				log.WithFields(fields).Info(filepath.Base(os.Args[0]))

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
