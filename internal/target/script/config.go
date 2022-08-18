// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package script

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/pflag"
)

// Config drives UserScript behavior.
type Config struct {
	FS       fs.FS  // A filesystem to load resources fs.
	MainPath string // A path, relative to FS that holds the entrypoint.

	userscript string // An external filesystem path.
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
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
