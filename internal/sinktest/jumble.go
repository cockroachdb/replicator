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

package sinktest

import (
	"math/rand"
	"strings"
	"unicode"

	"github.com/cockroachdb/replicator/internal/util/ident"
)

// JumbleIdent returns a case-jumbled version of the Ident.
func JumbleIdent(id ident.Ident) ident.Ident {
	var sb strings.Builder
	for _, r := range id.Raw() {
		if rand.Float32() < 0.5 {
			r = unicode.SimpleFold(r) // Replace with another case.
		}
		sb.WriteRune(r)
	}
	return ident.New(sb.String())
}

// JumbleSchema returns a case-jumbled version of the Schema.
func JumbleSchema(sch ident.Schema) ident.Schema {
	parts := sch.Idents(nil)
	for i, part := range parts {
		parts[i] = JumbleIdent(part)
	}
	ret, err := ident.NewSchema(parts...)
	if err != nil {
		panic(err)
	}
	return ret
}

// JumbleTable returns a case-jumbled version of the Table.
func JumbleTable(tbl ident.Table) ident.Table {
	return ident.NewTable(JumbleSchema(tbl.Schema()), JumbleIdent(tbl.Table()))
}
