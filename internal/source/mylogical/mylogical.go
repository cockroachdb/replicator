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

package mylogical

import (
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/stdlogical"
)

// MYLogical is a MySQL/MariaDB logical replication loop.
type MYLogical struct {
	Diagnostics *diag.Diagnostics
	Loop        *logical.Loop
}

var (
	_ stdlogical.HasDiagnostics = (*MYLogical)(nil)
	_ stdlogical.HasStoppable   = (*MYLogical)(nil)
)

// GetDiagnostics implements [stdlogical.HasDiagnostics].
func (l *MYLogical) GetDiagnostics() *diag.Diagnostics {
	return l.Diagnostics
}

// GetStoppable implements [stdlogical.HasStoppable].
func (l *MYLogical) GetStoppable() types.Stoppable {
	return l.Loop
}
