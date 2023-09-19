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

// Package scripttest contains reusable test helpers for
// logical-replication tests.
package scripttest

import (
	"embed"
	"io"
	"io/fs"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// ScriptFS contains the contents of the testdata directory.
//
//go:embed testdata/*
var ScriptFS embed.FS

// ScriptFSFor returns a wrapped version of ScriptFS that will
// substitute the table and its schema into the returned files.
func ScriptFSFor(tbl ident.Table) fs.FS {
	return &SubstitutingFS{
		FS: ScriptFS,
		Replacer: strings.NewReplacer(
			"{{ SCHEMA }}", tbl.Schema().Raw(),
			"{{ TABLE }}", tbl.Raw(),
		),
	}
}

// SubstitutingFS performs string substitution on returned files.
// This is used to replace sentinel values in the userscript with
// dynamically generated values.
type SubstitutingFS struct {
	fs.FS
	Replacer *strings.Replacer
}

// Open implements fs.FS.
func (f *SubstitutingFS) Open(path string) (fs.File, error) {
	file, err := f.FS.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	s := f.Replacer.Replace(string(buf))
	return &substitutingFile{info, io.NopCloser(strings.NewReader(s))}, nil
}

type substitutingFile struct {
	fs.FileInfo
	io.ReadCloser
}

var _ fs.File = (*substitutingFile)(nil)

func (s *substitutingFile) Stat() (fs.FileInfo, error) {
	return s.FileInfo, nil
}
