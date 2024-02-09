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
func (c *Config) Copy() *Config {
	ret := NewConfig()
	ret.Acceptor = c.Acceptor
	ret.CASColumns = append(ret.CASColumns, c.CASColumns...)
	c.Deadlines.CopyInto(ret.Deadlines)
	c.Exprs.CopyInto(ret.Exprs)
	ret.Extras = c.Extras
	c.Ignore.CopyInto(ret.Ignore)
	ret.Merger = c.Merger
	c.SourceNames.CopyInto(ret.SourceNames)

	return ret
}

// Equal returns true if the other Config is equivalent to the receiver.
//
// This method is intended for testing only. It does not compare the
// callback fields, since not all implementations of those interfaces
// are guaranteed to have a defined comparison operation (e.g.
// merge.Func).
func (c *Config) Equal(o *Config) bool {
	return c == o || // Identity or nil-nil.
		(c != nil) && (o != nil) &&
			// Not all implementations of Acceptor are comparable.
			c.CASColumns.Equal(o.CASColumns) &&
			c.Deadlines.Equal(o.Deadlines, cmap.Comparator[time.Duration]()) &&
			c.Exprs.Equal(o.Exprs, cmap.Comparator[string]()) &&
			ident.Equal(c.Extras, o.Extras) &&
			c.Ignore.Equal(o.Ignore, cmap.Comparator[bool]()) &&
			// Not all implementations of Merger are comparable: merge.Func or similar.
			c.SourceNames.Equal(o.SourceNames, ident.Comparator[ident.Ident]())
}

// IsZero returns true if the Config represents the absence of a
// configuration.
func (c *Config) IsZero() bool {
	return c.Acceptor == nil &&
		len(c.CASColumns) == 0 &&
		c.Deadlines.Len() == 0 &&
		c.Exprs.Len() == 0 &&
		c.Extras.Empty() &&
		c.Ignore.Len() == 0 &&
		c.Merger == nil &&
		c.SourceNames.Len() == 0
}

// Patch applies any non-empty fields from another Config to the
// receiver and returns the receiver.
func (c *Config) Patch(other *Config) *Config {
	if other.Acceptor != nil {
		c.Acceptor = other.Acceptor
	}
	c.CASColumns = append(c.CASColumns, other.CASColumns...)
	if other.Deadlines != nil {
		other.Deadlines.CopyInto(c.Deadlines)
	}
	if other.Exprs != nil {
		other.Exprs.CopyInto(c.Exprs)
	}
	if !other.Extras.Empty() {
		c.Extras = other.Extras
	}
	if other.Ignore != nil {
		other.Ignore.CopyInto(c.Ignore)
	}
	if other.Merger != nil {
		c.Merger = other.Merger
	}
	if other.SourceNames != nil {
		other.SourceNames.CopyInto(c.SourceNames)
	}
	return c
}
