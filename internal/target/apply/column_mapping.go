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

package apply

import (
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// A columnMapping is used to resolve payload keys to database columns
// that we intend to operate on. For instance, a CockroachDB changefeed
// generally uses lower-case keys in the JSON payloads, while a target
// database may default to upper-case names.
//
// The columnMapping also contains data about the target schema that we
// want to memoize.
type columnMapping struct {
	*ident.Map[positionalColumn]                    // Map of idents to column info and position.
	Conditions                   []types.ColData    // The version-like fields for CAS ops.
	Columns                      []types.ColData    // All columns named in an upsert statement.
	Data                         []types.ColData    // Non-PK, non-ignored columns.
	Deadlines                    types.Deadlines    // Allow too-old data to just be dropped.
	Exprs                        *ident.Map[string] // Value-replacement expressions.
	ExtrasColIdx                 int                // Position of the extras column, or -1 if unconfigured.
	Product                      types.Product      // Target database product.
	PK                           []types.ColData    // The names of the PK columns.
	PKDelete                     []types.ColData    // The names of the PK columns to delete.
	TableName                    ident.Table        // The target table.
	UpsertParameterCount         int                // The number of SQL arguments.
}

// positionalColumn augments ColData with the offset of the positional
// substitution parameter to be used within a batch of values.
type positionalColumn struct {
	types.ColData
	UpsertPosition int
}

func newColumnMapping(
	cfg *applycfg.Config, cols []types.ColData, product types.Product, table ident.Table,
) (*columnMapping, error) {
	ret := &columnMapping{
		Conditions:   make([]types.ColData, len(cfg.CASColumns)),
		Deadlines:    &ident.Map[time.Duration]{},
		Exprs:        &ident.Map[string]{},
		ExtrasColIdx: -1,
		Map:          &ident.Map[positionalColumn]{},
		Product:      product,
		TableName:    table,
	}

	// Map cas column names to their order in the comparison tuple.
	var casMap ident.Map[int]
	for idx, name := range cfg.CASColumns {
		casMap.Put(name, idx)
	}
	// Track the positional parameters for an upsert.
	currentParameterIndex := 0
	for _, col := range cols {
		if _, collision := ret.Get(col.Name); collision {
			return nil, errors.Errorf("column name collision: %s", col.Name)
		}

		// PK columns are always mentioned in DELETE statements.
		if col.Primary {
			ret.PKDelete = append(ret.PKDelete, col)
		}

		// Determine the positional parameter number for the column. A
		// negative value allows us to remember that the column exists,
		// but that we don't intend to do anything with incoming data
		// for that column.
		positionalParameterIndex := -1
		// We also determine whether the column appears in an upsert
		// statement at all.
		willUpsert := false
		if col.Ignored {
			// col.Ignored is true for generated columns. That field is
			// driven by inspecting the target schema.
		} else if cfg.Ignore.GetZero(col.Name) {
			// The user can elect to ignore certain incoming data to
			// facilitate schema changes.
		} else if expr, ok := cfg.Exprs.Get(col.Name); ok &&
			!strings.Contains(expr, applycfg.SubstitutionToken) {
			// We allow the user to specify an arbitrary expression for
			// a column value. If there's no $0 substitution token, then
			// the template will bake in a fixed expression.
			willUpsert = true
		} else {
			positionalParameterIndex = currentParameterIndex
			currentParameterIndex++
			willUpsert = true
		}

		// If a column is not part of this map and there is no extras
		// column configured, we'll return an error to the caller. This
		// ensures that all data that is part of a mutation has
		// somewhere to go or has been explicitly ignored, either by the
		// target database or by the user.
		ret.Put(col.Name, positionalColumn{col, positionalParameterIndex})

		if !willUpsert {
			continue
		}

		// Assemble columns in their intended uses.
		ret.Columns = append(ret.Columns, col)
		if col.Primary {
			ret.PK = append(ret.PK, col)
		} else {
			ret.Data = append(ret.Data, col)
		}
		if idx, isCas := casMap.Get(col.Name); isCas {
			ret.Conditions[idx] = col
		}
		// This has the side effect of ensuring that the Deadlines map
		// contains the target's exact column identifier.
		if deadline, ok := cfg.Deadlines.Get(col.Name); ok {
			ret.Deadlines.Put(col.Name, deadline)
		}
		if expr, ok := cfg.Exprs.Get(col.Name); ok {
			ret.Exprs.Put(col.Name, expr)
		}
		if ident.Equal(col.Name, cfg.Extras) {
			ret.ExtrasColIdx = positionalParameterIndex
		}
	}
	ret.UpsertParameterCount = currentParameterIndex

	// We also allow the user to force non-existent columns to be
	// ignored (e.g. to drop a column).
	_ = cfg.Ignore.Range(func(tgt ident.Ident, _ bool) error {
		if _, alreadyIgnored := ret.Get(tgt); !alreadyIgnored {
			ret.Put(tgt, positionalColumn{
				ColData: types.ColData{
					Ignored: true,
					Name:    tgt,
					Type:    "VOID",
				},
				UpsertPosition: -1,
			})
		}
		return nil
	})

	// Add redundant mappings for renamed columns.
	if err := cfg.SourceNames.Range(func(tgt ident.Ident, src applycfg.SourceColumn) error {
		if found, ok := ret.Get(tgt); ok {
			ret.Put(src, found)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return ret, nil
}
