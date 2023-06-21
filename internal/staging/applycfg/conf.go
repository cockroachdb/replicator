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

package applycfg

import (
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// SubstitutionToken contains the string that we'll use to substitute in
// the actual parameter index into the generated SQL.
const SubstitutionToken = "$0"

// Type aliases to improve readability.
type (
	// SourceColumn is the name of a column found in incoming data.
	SourceColumn = ident.Ident
	// TargetColumn is the name of a column found in the target database.
	TargetColumn = ident.Ident
)

// A Config contains per-target-table configuration.
type Config struct {
	CASColumns  []TargetColumn                 // The columns for compare-and-set operations.
	Deadlines   map[TargetColumn]time.Duration // Deadline-based operation.
	Exprs       map[TargetColumn]string        // Synthetic or replacement SQL expressions.
	Extras      TargetColumn                   // JSONB column to store unmapped values in.
	Ignore      map[TargetColumn]bool          // Source column names to ignore.
	SourceNames map[TargetColumn]SourceColumn  // Look for alternate name in the incoming data.
}

// NewConfig constructs a Config with all map fields populated.
func NewConfig() *Config {
	return &Config{
		Deadlines:   make(types.Deadlines),
		Exprs:       make(map[TargetColumn]string),
		Ignore:      make(map[TargetColumn]bool),
		SourceNames: make(map[TargetColumn]SourceColumn),
	}
}

// Copy returns a copy of the Config.
func (t *Config) Copy() *Config {
	ret := NewConfig()

	ret.CASColumns = append(ret.CASColumns, t.CASColumns...)
	for k, v := range t.Deadlines {
		ret.Deadlines[k] = v
	}
	for k, v := range t.Exprs {
		ret.Exprs[k] = v
	}
	ret.Extras = t.Extras
	for k, v := range t.Ignore {
		ret.Ignore[k] = v
	}
	for k, v := range t.SourceNames {
		ret.SourceNames[k] = v
	}

	return ret
}

// IsZero returns true if the Config represents the absence of a
// configuration.
func (t *Config) IsZero() bool {
	return len(t.CASColumns) == 0 &&
		len(t.Deadlines) == 0 &&
		len(t.Exprs) == 0 &&
		t.Extras.IsEmpty() &&
		len(t.Ignore) == 0 &&
		len(t.SourceNames) == 0
}
