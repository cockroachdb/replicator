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

package conveyor

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
)

type acceptReader struct {
	ch chan *types.BatchCursor
}

var (
	_ types.BatchReader   = (*acceptReader)(nil)
	_ types.MultiAcceptor = (*acceptReader)(nil)
)

func (a *acceptReader) AcceptMultiBatch(ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions) error {
	for _, temp := range batch.Data {
		if err := a.AcceptTemporalBatch(ctx, temp, opts); err != nil {
			return err
		}
	}
	return nil
}

func (a *acceptReader) AcceptTableBatch(ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions) error {
	temp := &types.TemporalBatch{Time: batch.Time}
	temp.Data.Put(batch.Table, batch)
	return a.AcceptTemporalBatch(ctx, temp, opts)
}

func (a *acceptReader) AcceptTemporalBatch(ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions) error {
	outcome := make(chan error, 1)
	cur := &types.BatchCursor{Batch: batch, Tag: outcome}
	a.ch <- cur
	select {
	case err := <-outcome:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *acceptReader) Read(_ *stopper.Context) (<-chan *types.BatchCursor, error) {
	return a.ch, nil
}

func (a *acceptReader) Terminal(_ *stopper.Context, tag any, err error) error {
	ch := tag.(chan error)
	ch <- err
	close(ch)
	return err
}
