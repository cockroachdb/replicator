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

package merge

import (
	"strings"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// ValidateNoUnmappedColumns returns an error message if the bag
// contains any unmapped properties.
func ValidateNoUnmappedColumns(bag *Bag) error {
	if bag.Unmapped.Len() == 0 {
		return nil
	}

	var unexpectedCols strings.Builder
	_ = bag.Unmapped.Range(func(name ident.Ident, _ any) error {
		if unexpectedCols.Len() > 0 {
			unexpectedCols.WriteString(", ")
		}
		unexpectedCols.WriteString(name.Raw())
		return nil
	})
	return errors.Errorf("unexpected columns: %s", unexpectedCols.String())
}

// ValidatePK ensures that the Bag has non-nil values for each primary
// key defined in its schema. This function will also return an error if
// the Bag has no schema.
//
// This function assumes that the primary key columns will be sorted
// first in the Bag's column data.
func ValidatePK(bag *Bag) error {
	missingPKs := &ident.Map[struct{}]{}
	for _, col := range bag.Columns {
		if !col.Primary {
			break
		}
		if !bag.Mapped.GetZero(col.Name).Valid {
			missingPKs.Put(col.Name, struct{}{})
		}
	}

	if missingPKs.Len() == 0 {
		return nil
	}

	var missingCols strings.Builder
	_ = missingPKs.Range(func(name ident.Ident, _ struct{}) error {
		if missingCols.Len() > 0 {
			missingCols.WriteString(", ")
		}
		missingCols.WriteString(name.Raw())
		return nil
	})
	return errors.Errorf("missing PK columns: %s", missingCols.String())
}
