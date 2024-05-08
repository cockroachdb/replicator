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

package pglogical

import (
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/stdlogical"
)

// PGLogical is a PostgreSQL logical replication loop.
type PGLogical struct {
	Conn        *Conn
	Diagnostics *diag.Diagnostics
	Memo        types.Memo // Support testing.
}

var (
	_ stdlogical.HasDiagnostics = (*PGLogical)(nil)
)

// GetDiagnostics implements [stdlogical.HasDiagnostics].
func (l *PGLogical) GetDiagnostics() *diag.Diagnostics {
	return l.Diagnostics
}
