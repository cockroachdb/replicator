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

package seqtest

import (
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/batches"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// Fragment breaks a batch up into a number of minimum-sized batches
// which are representative of how the bulk-transfer CDC feed delivers
// data (i.e. payloads per table). The data for any given table will
// remain in a time-ordered fashion.
func Fragment(batch *types.MultiBatch, windowSize int) ([]*types.MultiBatch, error) {
	var ret []*types.MultiBatch

	byTable := types.FlattenByTable[*types.MultiBatch](batch)
	if err := byTable.Range(func(table ident.Table, muts []types.Mutation) error {
		return batches.Window(windowSize, len(muts), func(begin, end int) error {
			next := &types.MultiBatch{}
			ret = append(ret, next)
			for _, mut := range muts[begin:end] {
				if err := next.Accumulate(table, mut); err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return ret, nil
}
