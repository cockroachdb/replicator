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

// Package subfs contains as [fs.FS] that performs string substitutions.
package subfs

import (
	"io"
	"io/fs"
	"strings"
)

// SubstitutingFS performs string substitution on returned files.
// This is used to replace sentinel values in the userscript with
// dynamically generated values.
type SubstitutingFS struct {
	fs.FS
	Replacer *strings.Replacer
}

// Open implements fs.FS. It will read the contents of the requested
// file and invoke the configured Replacer.
func (f *SubstitutingFS) Open(path string) (fs.File, error) {
	file, err := f.FS.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	s := f.Replacer.Replace(string(buf))
	return &substitutingFile{info, strings.NewReader(s)}, nil
}

// substitutingFile implements fs.File in addition to the recommended
// capability interfaces.
type substitutingFile struct {
	fs.FileInfo
	*strings.Reader
}

var (
	_ fs.File     = (*substitutingFile)(nil)
	_ io.ReaderAt = (*substitutingFile)(nil)
	_ io.Seeker   = (*substitutingFile)(nil)
)

// Close implements fs.File and is a no-op.
func (s *substitutingFile) Close() error { return nil }

// Stat implements fs.File.
func (s *substitutingFile) Stat() (fs.FileInfo, error) {
	return s.FileInfo, nil
}
