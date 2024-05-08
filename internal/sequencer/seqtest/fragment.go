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
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// Fragment breaks a batch up into a number of minimum-sized batches
// which are representative of how the bulk-transfer CDC feed delivers
// data (i.e. payloads per table). The data for any given table will
// remain in a time-ordered fashion.
func Fragment(batch *types.MultiBatch) ([]*types.MultiBatch, error) {
	var tableBatches ident.TableMap[*types.MultiBatch]
	if err := batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		tableMulti := tableBatches.GetZero(table)
		if tableMulti == nil {
			tableMulti = &types.MultiBatch{}
			tableBatches.Put(table, tableMulti)
		}
		return tableMulti.Accumulate(table, mut)
	})); err != nil {
		return nil, err
	}

	ret := make([]*types.MultiBatch, 0, tableBatches.Len())
	if err := tableBatches.Range(func(table ident.Table, tableMulti *types.MultiBatch) error {
		ret = append(ret, tableMulti)
		return nil
	}); err != nil {
		return nil, err
	}

	return ret, nil
}
