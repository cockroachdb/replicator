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
	"github.com/cockroachdb/field-eng-powertools/notify"
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

// A CannedReader always replays the same mutations.
type CannedReader struct {
	Data       notify.Var[[]*types.TemporalBatch] // The data to emit.
	Idle       notify.Var[struct{}]               // Notified when all cursors have been sent.
	ProgressTo notify.Var[hlc.Range]              // Emitted when idle.
}

var _ types.BatchReader = (*CannedReader)(nil)

// Read implements [types.BatchReader].
func (r *CannedReader) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	out := make(chan *types.BatchCursor, 2)
	ctx.Go(func(ctx *stopper.Context) error {
		defer close(out)
		for {
			dataChanged, _ := r.Data.Peek(func(data []*types.TemporalBatch) error {
				for _, batch := range data {
					cur := &types.BatchCursor{
						Batch:    batch,
						Progress: hlc.RangeIncluding(hlc.Zero(), batch.Time),
					}
					select {
					case out <- cur:
					case <-ctx.Stopping():
						return nil
					}
				}
				return nil
			})

			progress, progressChanged := r.ProgressTo.Get()
			cur := &types.BatchCursor{
				Progress: progress,
			}
			select {
			case out <- cur:
			case <-ctx.Stopping():
				return nil
			}

			r.Idle.Notify()

			select {
			case <-dataChanged:
			case <-progressChanged:
			case <-ctx.Stopping():
				return nil
			}
		}
	})
	return out, nil
}
