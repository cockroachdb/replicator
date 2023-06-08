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
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/pflag"
)

// Config drives UserScript behavior.
type Config struct {
	FS       fs.FS   // A filesystem to load resources fs.
	MainPath string  // A path, relative to FS that holds the entrypoint.
	Options  Options // The target for calls to api.setOptions().

	userscript string // An external filesystem path.
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	if c.Options == nil {
		c.Options = &FlagOptions{f}
	}
	f.StringVar(&c.userscript, "userscript", "",
		"the path to a configuration script, see userscript subcommand")
}

// Preflight validates the configuration.
func (c *Config) Preflight() error {
	if c.userscript != "" {
		path, err := filepath.Abs(c.userscript)
		if err != nil {
			return err
		}

		dir, path := filepath.Split(path)
		c.FS = os.DirFS(dir)
		c.MainPath = "/" + path
		c.userscript = ""
	}

	return nil
}
