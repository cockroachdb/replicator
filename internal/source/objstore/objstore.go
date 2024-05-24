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

// Package objstore retrieves changefeed objects stored in a cloud storage and applies them to a
// target.
package objstore

import (
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/stdlogical"
)

// Objstore is a logical replication loop that uses cloud storage.
type Objstore struct {
	Conn        *Conn
	Diagnostics *diag.Diagnostics
}

var (
	_ stdlogical.HasDiagnostics = (*Objstore)(nil)
)

// GetDiagnostics implements [stdlogical.HasDiagnostics].
func (k *Objstore) GetDiagnostics() *diag.Diagnostics {
	return k.Diagnostics
}
