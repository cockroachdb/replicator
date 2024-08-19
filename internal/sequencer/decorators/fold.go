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

package decorators

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// Fold constructs facades around acceptors that elide all but the last
// update to any particular row. This allows the wrapped delegate to
// effectively skip over mutations that would not have visible
// side-effects when applied as a single transaction.
type Fold struct{}

// MultiAcceptor returns a folding facade around the delegate.
func (f *Fold) MultiAcceptor(acceptor types.MultiAcceptor) types.MultiAcceptor {
	return &fold{
		base: base{
			multiAcceptor:    acceptor,
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
	}
}

// TableAcceptor returns a folding facade around the delegate.
func (f *Fold) TableAcceptor(acceptor types.TableAcceptor) types.TableAcceptor {
	return &fold{
		base: base{
			tableAcceptor: acceptor,
		},
	}
}

// TemporalAcceptor returns a folding facade around the delegate.
func (f *Fold) TemporalAcceptor(acceptor types.TemporalAcceptor) types.TemporalAcceptor {
	return &fold{
		base: base{
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
	}
}

type fold struct {
	base
}

var _ types.MultiAcceptor = (*fold)(nil)

func (f *fold) AcceptTableBatch(ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions) error {
	if f.tableAcceptor == nil {
		return errors.New("no tableAcceptor set")
	}

	next := &types.TableBatch{}
	if err := FoldInto[*types.TableBatch](batch, next); err != nil {
		return err
	}

	return f.tableAcceptor.AcceptTableBatch(ctx, next, opts)
}

func (f *fold) AcceptTemporalBatch(ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions) error {
	if f.temporalAcceptor == nil {
		return errors.New("no temporalAcceptor set")
	}

	next := &types.TemporalBatch{}
	if err := FoldInto[*types.TemporalBatch](batch, next); err != nil {
		return err
	}

	return f.temporalAcceptor.AcceptTemporalBatch(ctx, next, opts)
}

func (f *fold) AcceptMultiBatch(ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions) error {
	if f.multiAcceptor == nil {
		return errors.New("no multiAcceptor set")
	}

	next := &types.MultiBatch{}
	if err := FoldInto[*types.MultiBatch](batch, next); err != nil {
		return err
	}

	return f.multiAcceptor.AcceptMultiBatch(ctx, next, opts)
}

func FoldInto[B any](batch types.Batch[B], acc types.Accumulator) error {
	var last ident.TableMap[map[string]types.Mutation]
	if err := batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		m, ok := last.Get(table)
		if !ok {
			m = make(map[string]types.Mutation)
			last.Put(table, m)
		}
		m[string(mut.Key)] = mut
		return nil
	})); err != nil {
		return err
	}

	return last.Range(func(table ident.Table, muts map[string]types.Mutation) error {
		for _, mut := range muts {
			if err := acc.Accumulate(table, mut); err != nil {
				return err
			}
		}
		return nil
	})
}
