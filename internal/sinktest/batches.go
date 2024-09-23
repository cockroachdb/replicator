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

package sinktest

import (
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// TableBatchOf is a convenience method for creating a
// [types.TableBatch] instance. The timestamp of each mutation will be
// set to the time argument, to easily create a well-formed batch.
func TableBatchOf(table ident.Table, time hlc.Time, data []types.Mutation) *types.TableBatch {
	ret := &types.TableBatch{
		Data:  data,
		Table: table,
		Time:  time,
	}
	for idx := range ret.Data {
		ret.Data[idx].Time = time
	}
	return ret
}

// An EmptyBatchReader never emits any data.
type EmptyBatchReader struct{}

var _ types.BatchReader = (*EmptyBatchReader)(nil)

// Read emits no data. The channel will be closed when the context is
// stopped.
func (r *EmptyBatchReader) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	ret := make(chan *types.BatchCursor)
	ctx.Defer(func() {
		close(ret)
	})
	return ret, nil
}
