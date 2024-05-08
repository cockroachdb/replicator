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

package script

import (
	_ "embed" // Embed API and example from test code.
	"fmt"

	"github.com/spf13/cobra"
)

var (
	//go:embed testdata/replicator@v1.d.ts
	bindings string
	//go:embed testdata/main.ts
	example string
)

const help = `
A userscript is a JavaScript / TypeScript program that allows arbitrary
logic to be injected into Replicator. It is especially useful in cases
where the source data does not map directly onto SQL tables (e.g.
migrations from a document store).

The contents of the userscript directory and its subdirectories are
accessible via import or require(). Security-minded users should place
the userscript into its own directory.

A non-trivial userscript may be precompiled using any JS/TS workflow
that produces a CommonJS-compatible output. Please note that, at the
current time, the JavaScript runtime does not support all ES6+ features,
especially those related to async behavior.

Re-run this command with the --api flag to print only the .d.ts file.
`

// HelpCommand returns an extended help command to print  TypeScript
// bindings for the userscript API.
func HelpCommand() *cobra.Command {
	var justAPI bool
	ret := &cobra.Command{
		Short: "print userscript help information",
		Use:   "userscript",
		Run: func(_ *cobra.Command, _ []string) {
			if justAPI {
				fmt.Print(bindings)
				return
			}
			fmt.Print(help)
			fmt.Print("\n\n===== VVV replicator@v1.d.ts VVV =====\n\n")
			fmt.Print(bindings)
			fmt.Print("\n\n===== VVV example-userscript.ts VVV ===== \n\n")
			fmt.Print(example)
			fmt.Print("\n")
		},
	}
	ret.Flags().BoolVar(&justAPI, "api", false,
		"write just the API .d.ts file to stdout.")
	return ret
}
