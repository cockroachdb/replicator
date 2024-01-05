// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Package db2 contains support for reading a db2 sql replication feed.
package db2

import (
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/stdlogical"
)

// DB2 is a logical replication loop for the IBM DB2 database.
// It leverages SQL replication on the source.
type DB2 struct {
	Diagnostics *diag.Diagnostics
	Loop        *logical.Loop
}

var (
	_ stdlogical.HasDiagnostics = (*DB2)(nil)
)

// GetDiagnostics implements [stdlogical.HasDiagnostics].
func (d *DB2) GetDiagnostics() *diag.Diagnostics {
	return d.Diagnostics
}
