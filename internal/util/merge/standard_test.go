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
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

// TestStandardMerge is a test for coverage.
func TestStandardMerge(t *testing.T) {
	cols := []types.ColData{
		{
			Name:    ident.New("pk0"),
			Primary: true,
			Type:    "INT8",
		},
		{
			Name:    ident.New("pk1"),
			Primary: true,
			Type:    "INT8",
		},
		{
			Name: ident.New("col0"),
			Type: "INT8",
		},
		{
			Name: ident.New("col1"),
			Type: "INT8",
		},
	}

	tcs := []struct {
		merger    Merger
		con       *Conflict
		expect    *Resolution
		expectErr string
	}{
		{
			// Trivial case.
			merger: &Standard{},
			con: &Conflict{
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
			},
		},
		{
			// Empty blocking row. We don't expect to actually see this
			// case, since there won't be a blocking row in the table.
			merger: &Standard{},
			con: &Conflict{
				Before: NewBagOf(cols, nil),
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
				Target: NewBagOf(cols, nil),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
			},
		},
		{
			// Delete col1.
			merger: &Standard{},
			con: &Conflict{
				Before: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 1,
				),
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 1,
				),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
				),
			},
		},
		{
			// Set col1 explicitly to nil.
			merger: &Standard{},
			con: &Conflict{
				Before: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 1,
				),
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", nil,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 1,
				),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", nil,
				),
			},
		},
		{
			// col1 has changed in the input.
			merger: &Standard{},
			con: &Conflict{
				Before: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 1,
				),
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 1,
				),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 42,
				),
			},
		},
		{
			// There is a conflict in col0
			merger: &Standard{},
			con: &Conflict{
				Before: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 99,
					"col1", 1,
				),
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 1,
				),
			},
			expectErr: `"Unmerged":["col0"]`,
		},
		{
			// Merge unmapped properties.
			merger: &Standard{},
			con: &Conflict{
				Before: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 1,
					"unmapped", false,
				),
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
					"unmapped", true,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 1,
					"existing_unmapped", true,
				),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 42,
					"unmapped", true,
					"existing_unmapped", true,
				),
			},
		},
		{
			// There is a conflict in col0 and a DLQ defined.
			merger: &Standard{
				Fallback: DLQ("dead"),
			},
			con: &Conflict{
				Before: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 99,
					"col1", 1,
				),
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 1000,
					"col1", 1,
				),
			},
			expect: &Resolution{DLQ: "dead"},
		},
		{
			// No before data, which will happen in an insert case.
			merger: &Standard{},
			con: &Conflict{
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 99,
					"col1", 101,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
			},
			expectErr: `"Unmerged":["col0","col1"]`,
		},
		{
			// No before data, but the update is a no-op.
			merger: &Standard{},
			con: &Conflict{
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
			},
		},
		{
			// No before data, but the update is a no-op for overlapping properties.
			merger: &Standard{},
			con: &Conflict{
				Proposed: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
					"foo", 99,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
					"bar", 101,
				),
			},
			expect: &Resolution{
				Apply: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
					"foo", 99,
					"bar", 101,
				),
			},
		},
		{
			// Silly error: No proposed data.
			merger: &Standard{},
			con: &Conflict{
				Before: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
				Target: NewBagOf(cols, nil,
					"pk0", 0,
					"pk1", 1,
					"col0", 0,
					"col1", 42,
				),
			},
			expectErr: "no proposed data",
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			res, err := tc.merger.Merge(context.Background(), tc.con)
			if tc.expectErr != "" {
				a.ErrorContains(err, tc.expectErr)
			} else if a.NoError(err) {
				eq, err := canonicalEquals(tc.expect, res)
				if a.NoError(err) {
					a.Truef(eq, "expected: %#v \n\n actual: %#v", tc.expect, res)
				}
			}
		})
	}
}
