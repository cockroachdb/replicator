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

// Package local provide access to local storage.
package local

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/pkg/errors"
)

// Config specifies the parameters required to create a bucket reader.
type Config struct {
	Directory string // Root directory
}

// New creates a bucket reader for a local filesystem.
func New(config *Config) (bucket.Reader, error) {
	return &localBucket{
		filesystem: os.DirFS(config.Directory),
	}, nil
}

// localBucket is a bucket backed by a filesystem.
type localBucket struct {
	filesystem fs.FS
}

var _ bucket.Reader = &localBucket{}

// Iter implements bucket.Reader
func (b *localBucket) Iter(
	ctx context.Context, dir string, f func(string) error, options bucket.IterOptions,
) error {
	count := 0
	return b.iter(ctx, dir, f, &count, options)
}

// Get implements bucket.Reader
func (b *localBucket) Get(ctx context.Context, file string) (io.ReadCloser, error) {
	return b.fs().Open(filepath.Clean(file))
}

// fs returns the underlying filesystem.
func (b *localBucket) fs() fs.FS {
	return b.filesystem
}

// iter recursively scans the entries in the filesystem, calling the f function
// for each file that matches the named options.
func (b *localBucket) iter(
	ctx context.Context, dir string, f func(string) error, count *int, options bucket.IterOptions,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if options.Max > 0 && *count > options.Max {
		return nil
	}
	dir = filepath.Clean(dir)
	info, err := fs.Stat(b.fs(), dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "stat %s", dir)
	}
	if !info.IsDir() {
		return nil
	}
	files, err := fs.ReadDir(b.fs(), dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		name := filepath.Join(dir, file.Name())
		if options.Max > 0 && *count >= options.Max {
			break
		}
		if file.IsDir() && options.Recursive {
			if err := b.iter(ctx, name, f, count, options); err != nil {
				return err
			}
			continue
		}
		fmt.Printf("compare %s %s\n", name, options.StartAfter)
		if options.StartAfter != "" && strings.Compare(name, options.StartAfter) <= 0 {
			continue
		}
		if err := f(name); err != nil {
			return err
		}
		*count++
	}
	return nil
}
