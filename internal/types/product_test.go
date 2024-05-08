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

package types

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestProductExpand(t *testing.T) {
	tcs := []struct {
		input    ident.Schema
		product  Product
		expected ident.Schema
		err      string
	}{
		{
			input: ident.Schema{},
			err:   "empty schema not allowed",
		},
		{
			input:   ident.MustSchema(ident.New("foo")),
			product: ProductUnknown,
			err:     "unimplemented",
		},
		{
			input:    ident.MustSchema(ident.New("foo")),
			product:  ProductCockroachDB,
			expected: ident.MustSchema(ident.New("foo"), ident.Public),
		},
		{
			input:    ident.MustSchema(ident.New("foo"), ident.New("bar")),
			product:  ProductCockroachDB,
			expected: ident.MustSchema(ident.New("foo"), ident.New("bar")),
		},
		// ident.NewSchema will return an error for 3-part names.
		{
			input:    ident.MustSchema(ident.New("foo")),
			product:  ProductOracle,
			expected: ident.MustSchema(ident.New("foo")),
		},
		{
			input:   ident.MustSchema(ident.New("foo"), ident.New("bar")),
			product: ProductOracle,
			err:     "expecting exactly one schema part",
		},
		{
			input:    ident.MustSchema(ident.New("foo")),
			product:  ProductMySQL,
			expected: ident.MustSchema(ident.New("foo")),
		},
		{
			input:   ident.MustSchema(ident.New("foo"), ident.New("bar")),
			product: ProductMySQL,
			err:     "expecting exactly one schema part",
		},
		{
			input:    ident.MustSchema(ident.New("foo")),
			product:  ProductMariaDB,
			expected: ident.MustSchema(ident.New("foo")),
		},
		{
			input:   ident.MustSchema(ident.New("foo"), ident.New("bar")),
			product: ProductMariaDB,
			err:     "expecting exactly one schema part",
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			r := require.New(t)
			sch, err := tc.product.ExpandSchema(tc.input)
			if tc.err == "" {
				r.NoError(err)
				r.True(ident.Equal(tc.expected, sch))
			} else {
				r.ErrorContains(err, tc.err)
			}
		})
	}
}
