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
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// AddMeta decorates the mutation with a standard set of properties.
func AddMeta(source string, tbl ident.Table, mut *types.Mutation) {
	meta := map[string]any{
		source:    true,
		"logical": mut.Time.Logical(),
		"nanos":   mut.Time.Nanos(),
		"schema":  tbl.Schema().Raw(),
		"table":   tbl.Table().Raw(),
	}
	// Almost always the case.
	if mut.Meta == nil {
		mut.Meta = meta
		return
	}
	for k, v := range meta {
		mut.Meta[k] = v
	}
}

// SourceName returns a standardized representation of a source name.
func SourceName(target ident.Schematic) ident.Ident {
	return ident.New(target.Schema().Canonical().Raw())
}
