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

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	commandPattern = regexp.MustCompile(`(?m)^\s*<!--\s*marksub:\s*(\S+)\s*-->\s*$`)
	blockPattern   = regexp.MustCompile("(?m)^```.*$")
)

type (
	modeFindCommand struct{}
	modeLookAhead   struct{ path string }
	modeDiscard     struct{ marker []byte }
)

func main() {
	root := &cobra.Command{
		Use:  "marksub <directory>",
		Long: "update code blocks in markdown files",
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var basepath string
			var err error

			switch len(args) {
			case 0:
				basepath, err = os.Getwd()
			case 1:
				basepath = args[0]
			}
			if err != nil {
				return err
			}
			basepath, err = filepath.Abs(basepath)
			if err != nil {
				return err
			}
			return processFiles(cmd.Context(), basepath)
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := root.ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}

// contentsEqual returns true if the two files contain the same data.
func contentsEqual(aPath, bPath string) (bool, error) {
	aF, err := os.Open(aPath)
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer aF.Close()

	aStat, err := aF.Stat()
	if err != nil {
		return false, errors.WithStack(err)
	}

	bF, err := os.Open(bPath)
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer bF.Close()

	bStat, err := bF.Stat()
	if err != nil {
		return false, errors.WithStack(err)
	}

	// Check the size.
	if aStat.Size() != bStat.Size() {
		return false, nil
	}

	// Otherwise, perform a byte-wise comparison of the files.
	aBuf := bufio.NewReader(aF)
	bBuf := bufio.NewReader(bF)
	for {
		aB, err := aBuf.ReadByte()
		if err != nil {
			// We know the file size is the same, so we don't care about
			// trying to read the EOF from the bBuf.
			if errors.Is(err, io.EOF) {
				return true, nil
			}
			return false, errors.WithStack(err)
		}
		bB, err := bBuf.ReadByte()
		if err != nil {
			return false, errors.WithStack(err)
		}
		if aB != bB {
			return false, nil
		}
	}
}

// processFiles walks basepath looking for .md files to process.
func processFiles(ctx context.Context, basepath string) error {
	return filepath.WalkDir(basepath, func(path string, d fs.DirEntry, err error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.Type().IsRegular() && filepath.Ext(path) == ".md" {
			return processFile(basepath, path)
		}
		return nil
	})
}

// processFile opens the markdown file at path to perform the requested
// substitution. The substituted data is written into a temporary file
// which will be deleted if no changes result.
func processFile(basepath string, path string) error {
	dir, file := filepath.Split(path)
	tempFile := filepath.Join(dir, fmt.Sprintf(".%s.tmp", file))
	out, err := os.Create(tempFile)
	if err != nil {
		return errors.WithStack(err)
	}
	subErr := substitute(basepath, path, out)
	if err := out.Close(); err != nil {
		return errors.WithStack(err)
	}
	if subErr != nil {
		_ = os.Remove(tempFile)
		return errors.WithStack(err)
	}

	// Determine if the input and output contain the same contents.
	eq, err := contentsEqual(path, tempFile)
	if err != nil {
		return errors.WithStack(err)
	}
	if eq {
		return os.Remove(tempFile)
	}

	log.Infof("updated %s", path)
	return os.Rename(tempFile, path)
}

// substitute reads the file at the given path, looking for substitution
// commands. When a command is found, the next code block will be
// replaced with the contents of the commanded file.
func substitute(basePath string, path string, out io.Writer) error {
	in, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, path)
	}

	mode := any(modeFindCommand{})
	// Used to sanity-check input paths.
	openBase, _ := filepath.Split(path)
	scanner := bufio.NewReader(in)

	for reading := true; reading; {
		line, err := scanner.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				reading = false
			} else {
				return errors.WithStack(err)
			}
		}

		switch m := mode.(type) {
		case modeFindCommand:
			// Look for command comments while copying output.
			if _, err := out.Write(line); err != nil {
				return errors.WithStack(err)
			}
			found := commandPattern.FindSubmatch(line)
			if found != nil {
				mode = modeLookAhead{string(found[1])}
			}

		case modeLookAhead:
			// Continue to copy output, but look for the start of a block that we'll substitute into.
			if _, err := out.Write(line); err != nil {
				return errors.WithStack(err)
			}
			// We've found the opening of the block to replace.
			if blockPattern.Match(line) {
				// Make the path absolute and sanity-check it.
				toOpen, err := filepath.Abs(filepath.Join(openBase, m.path))
				if err != nil {
					return errors.WithStack(err)
				}
				if !strings.HasPrefix(toOpen, basePath) {
					return errors.Errorf("target path %q not in base path %q", m.path, basePath)
				}
				toCopy, err := os.Open(toOpen)
				if err != nil {
					return errors.Wrap(err, toOpen)
				}
				_, err = io.Copy(out, toCopy)
				_ = toCopy.Close()
				if err != nil {
					return errors.Wrap(err, toOpen)
				}

				mode = modeDiscard{marker: []byte("```")}
			}

		case modeDiscard:
			if bytes.HasPrefix(line, m.marker) {
				if _, err := out.Write(line); err != nil {
					return errors.WithStack(err)
				}
				mode = modeFindCommand{}
			}

		default:
			panic("bad mode")
		}
	}
	if _, idle := mode.(modeFindCommand); !idle {
		return errors.New("unterminated input")
	}
	return nil
}
