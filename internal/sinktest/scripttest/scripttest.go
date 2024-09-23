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
	"io/fs"
	"strings"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/subfs"
)

// ScriptFS contains the contents of the testdata directory.
//
//go:embed testdata/*
var ScriptFS embed.FS

// ScriptFSFor returns a wrapped version of ScriptFS that will
// substitute the table and its schema into the returned files.
func ScriptFSFor(tbl ident.Table) fs.FS {
	return &subfs.SubstitutingFS{
		FS: ScriptFS,
		Replacer: strings.NewReplacer(
			"{{ SCHEMA }}", tbl.Schema().Raw(),
			"{{ TABLE }}", tbl.Raw(),
		),
	}
}

// ScriptFSParentChild returns a wrapped version of ScriptFS that will
// substitute the table and its schema into the returned files.
func ScriptFSParentChild(parent, child ident.Table) fs.FS {
	return &subfs.SubstitutingFS{
		FS: ScriptFS,
		Replacer: strings.NewReplacer(
			"{{ SCHEMA }}", parent.Schema().Raw(),
			"{{ PARENT }}", parent.Raw(),
			"{{ CHILD }}", child.Raw(),
		),
	}
}
