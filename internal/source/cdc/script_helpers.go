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

package cdc

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// scriptSource returns the canonical name of a schema for use with
// the configureSource() function in the userscript API.
func scriptSource(target ident.Schematic) ident.Ident {
	return ident.New(target.Schema().Canonical().Raw())
}

// scriptMeta create a userscript metadata map for a mutation being
// applied to a table.
func scriptMeta(tbl ident.Table, mut types.Mutation) map[string]any {
	return map[string]any{
		"cdc":     true,
		"logical": mut.Time.Logical(),
		"nanos":   mut.Time.Nanos(),
		"schema":  tbl.Schema().Raw(),
		"table":   tbl.Table().Raw(),
	}
}
