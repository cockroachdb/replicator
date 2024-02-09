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
	"github.com/cockroachdb/cdc-sink/internal/util/cmap"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/merge"
)

// SubstitutionToken contains the string that we'll use to substitute in
// the actual parameter index into the generated SQL.
const SubstitutionToken = "$0"

// Type aliases to improve readability.
type (
	// SourceColumn is the name of a column found in incoming data.
	SourceColumn = ident.Ident
	// SourceColumns is the names of columns found in the source database.
	SourceColumns = ident.Idents
	// TargetColumn is the name of a column found in the target database.
	TargetColumn = ident.Ident
	// TargetColumns is the names of columns found in the target database.
	TargetColumns = ident.Idents
)

// A Config contains per-target-table configuration.
type Config struct {
	// NB: Update TestCopyEquals if adding new fields.
	Acceptor    types.TableAcceptor       // Inject user-defined apply behavior instead.
	CASColumns  TargetColumns             // The columns for compare-and-set operations.
	Deadlines   *ident.Map[time.Duration] // Deadline-based operation.
	Exprs       *ident.Map[string]        // Synthetic or replacement SQL expressions.
	Extras      TargetColumn              // JSONB column to store unmapped values in.
	Ignore      *ident.Map[bool]          // Source column names to ignore.
	Merger      merge.Merger              // Conflict resolution.
	SourceNames *ident.Map[SourceColumn]  // Look for alternate name in the incoming data.
}

// NewConfig constructs a Config with all map fields populated.
func NewConfig() *Config {
	return &Config{
		Deadlines:   &ident.Map[time.Duration]{},
		Exprs:       &ident.Map[string]{},
		Ignore:      &ident.Map[bool]{},
		SourceNames: &ident.Map[SourceColumn]{},
	}
}

// Copy returns a copy of the Config.
func (t *Config) Copy() *Config {
	ret := NewConfig()
	ret.Acceptor = t.Acceptor
	ret.CASColumns = append(ret.CASColumns, t.CASColumns...)
	t.Deadlines.CopyInto(ret.Deadlines)
	t.Exprs.CopyInto(ret.Exprs)
	ret.Extras = t.Extras
	t.Ignore.CopyInto(ret.Ignore)
	ret.Merger = t.Merger
	t.SourceNames.CopyInto(ret.SourceNames)

	return ret
}

// Equal returns true if the other Config is equivalent to the receiver.
//
// This method is intended for testing only. It does not compare the
// callback fields, since not all implementations of those interfaces
// are guaranteed to have a defined comparison operation (e.g.
// merge.Func).
func (t *Config) Equal(o *Config) bool {
	return t == o || // Identity or nil-nil.
		(t != nil) && (o != nil) &&
			// Not all implementations of Acceptor are comparable.
			t.CASColumns.Equal(o.CASColumns) &&
			t.Deadlines.Equal(o.Deadlines, cmap.Comparator[time.Duration]()) &&
			t.Exprs.Equal(o.Exprs, cmap.Comparator[string]()) &&
			ident.Equal(t.Extras, o.Extras) &&
			t.Ignore.Equal(o.Ignore, cmap.Comparator[bool]()) &&
			// Not all implementations of Merger are comparable: merge.Func or similar.
			t.SourceNames.Equal(o.SourceNames, ident.Comparator[ident.Ident]())
}

// IsZero returns true if the Config represents the absence of a
// configuration.
func (t *Config) IsZero() bool {
	return t.Acceptor == nil &&
		len(t.CASColumns) == 0 &&
		t.Deadlines.Len() == 0 &&
		t.Exprs.Len() == 0 &&
		t.Extras.Empty() &&
		t.Ignore.Len() == 0 &&
		t.Merger == nil &&
		t.SourceNames.Len() == 0
}

// Patch applies any non-empty fields from another Config to the
// receiver and returns the receiver.
func (t *Config) Patch(other *Config) *Config {
	if other.Acceptor != nil {
		t.Acceptor = other.Acceptor
	}
	t.CASColumns = append(t.CASColumns, other.CASColumns...)
	if other.Deadlines != nil {
		other.Deadlines.CopyInto(t.Deadlines)
	}
	if other.Exprs != nil {
		other.Exprs.CopyInto(t.Exprs)
	}
	if !other.Extras.Empty() {
		t.Extras = other.Extras
	}
	if other.Ignore != nil {
		other.Ignore.CopyInto(t.Ignore)
	}
	if other.Merger != nil {
		t.Merger = other.Merger
	}
	if other.SourceNames != nil {
		other.SourceNames.CopyInto(t.SourceNames)
	}
	return t
}
