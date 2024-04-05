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

package stdpool

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestFTSHintSupported(t *testing.T) {
	tcs := []struct {
		version string
		hinted  bool
	}{
		{"v23.1.16", false},
		{"v23.1.17", true},
		{"v23.1.18", true},
		{"v23.2.0", false},
		{"v23.2.1", false},
		{"v23.2.2", false},
		{"v23.2.3", true},
		{"v23.2.4", true},
		{"v24.1.0", true},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			r := require.New(t)
			tc.version = fmt.Sprintf("CockroachDB CCL %s (platform)", tc.version)
			support, err := cockroachSupportsNoFullScanHint(tc.version)
			r.NoError(err)
			r.Equal(tc.hinted, support)
		})
	}
}

func TestFTSPoolInfo(t *testing.T) {
	tcs := []struct {
		version string
		hinted  bool
	}{
		{"v23.1.16", false},
		{"v23.1.17", true},
	}
	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			r := require.New(t)
			pi := &types.PoolInfo{
				Product: types.ProductCockroachDB,
				Version: fmt.Sprintf("CockroachDB CCL %s (platform)", tc.version),
			}
			r.NoError(setTableHint(pi))
			r.NotNil(pi.HintNoFTS)

			hint := pi.HintNoFTS(ident.Table{}).Hint
			if tc.hinted {
				r.Equal("@{NO_FULL_SCAN}", hint)
			} else {
				r.Empty(hint)
			}
		})
	}
	t.Run("non-crdb", func(t *testing.T) {
		r := require.New(t)
		pi := &types.PoolInfo{
			Product: types.ProductPostgreSQL,
			Version: "ignored",
		}
		r.NoError(setTableHint(pi))
		r.NotNil(pi.HintNoFTS)
		r.Empty(pi.HintNoFTS(ident.Table{}).Hint)
	})
}
