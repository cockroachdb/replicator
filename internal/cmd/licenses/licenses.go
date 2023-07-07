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

// Package licenses bundles the licence files for those go modules which
// are reachable from the main entry point.
package licenses

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

//go:generate go run github.com/google/go-licenses save ../../.. --save_path ./data/licenses --force

//go:embed data
var data embed.FS

// Command iterates through the embedded filesystem containing license
// notifications and prints a report.
func Command() *cobra.Command {
	return &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "print licenses for redistributed modules",
		Use:   "licenses",
		RunE: func(cmd *cobra.Command, args []string) error {
			const base = "data/licenses"
			return fs.WalkDir(data, base, func(path string, d fs.DirEntry, err error) error {
				switch {
				case os.IsNotExist(err):
					return errors.New("development binaries should not be distributed")
				case err != nil:
					return err
				case d.IsDir():
					return nil
				}

				// Trim leading "data/" and trailing slash.
				pkg, _ := filepath.Split(path)
				fmt.Printf("Module %s:\n\n", pkg[len(base)+1:len(pkg)-1])

				f, err := data.Open(path)
				if err != nil {
					return errors.Wrap(err, path)
				}
				defer f.Close()
				if _, err := io.Copy(os.Stdout, f); err != nil {
					return errors.Wrap(err, path)
				}
				fmt.Print("--------\n\n")
				return nil
			})
		},
	}
}
