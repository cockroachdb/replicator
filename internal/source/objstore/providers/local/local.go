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
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
)

// New creates a bucket reader for a local filesystem.
func New(fs fs.FS) (bucket.Bucket, error) {
	return &localBucket{
		filesystem: fs,
	}, nil
}

// localBucket is a bucket backed by a filesystem.
type localBucket struct {
	filesystem fs.FS
}

var _ bucket.Bucket = &localBucket{}

// Iter implements bucket.Reader
func (b *localBucket) Walk(
	ctx *stopper.Context,
	dir string,
	options *bucket.WalkOptions,
	f func(*stopper.Context, string) error,
) error {
	count := 0
	dir = filepath.Clean(dir)
	err := fs.WalkDir(b.filesystem, dir,
		func(path string, d fs.DirEntry, err error) error {
			if options.StartAfter != "" && strings.Compare(path, options.StartAfter) <= 0 {
				return nil
			}
			if options.Limit > 0 && count >= options.Limit {
				return fs.SkipAll
			}
			if d != nil && d.IsDir() {
				if options.Recursive {
					return nil
				}
				return fs.SkipDir
			}
			count++
			return f(ctx, path)
		})
	if errors.Is(err, bucket.ErrSkipAll) {
		return nil
	}
	return err
}

// Open implements bucket.Reader
func (b *localBucket) Open(ctx *stopper.Context, file string) (io.ReadCloser, error) {
	r, err := b.fs().Open(filepath.Clean(file))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, errors.Join(bucket.ErrNoSuchKey, err)
	}
	return r, err
}

// fs returns the underlying filesystem.
func (b *localBucket) fs() fs.FS {
	return b.filesystem
}
