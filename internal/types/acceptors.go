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

package types

// This file defines various interfaces which accept the batch types.

import (
	"context"

	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// AcceptOptions is an API escape hatch to provide hints or other
// metadata to acceptor implementations.
type AcceptOptions struct {
	TargetQuerier TargetQuerier // Override the target database access.
}

// Copy returns a copy of the options.
func (o *AcceptOptions) Copy() *AcceptOptions {
	ret := *o
	return &ret
}

// A MultiAcceptor operates on a MultiBatch to achieve some effect within
// the target.
type MultiAcceptor interface {
	TemporalAcceptor
	// AcceptMultiBatch processes the batch. The options may be nil.
	AcceptMultiBatch(context.Context, *MultiBatch, *AcceptOptions) error
}

// OrderedAcceptorFrom will return an adaptor which iterates over tables
// in the table-dependency order. The returned acceptor will respect the
// [AcceptOptions.TableOrder] when iterating within a [TemporalBatch].
func OrderedAcceptorFrom(acc TableAcceptor, watchers Watchers) MultiAcceptor {
	return &orderedAdapter{acc, watchers}
}

// A TemporalAcceptor operates on a batch of data that has a single
// timestamp (i.e. a source database transaction).
type TemporalAcceptor interface {
	TableAcceptor
	// AcceptTemporalBatch processes the batch. The options may be nil.
	AcceptTemporalBatch(context.Context, *TemporalBatch, *AcceptOptions) error
}

// A TableAcceptor operates on a TableBatch.
type TableAcceptor interface {
	// AcceptTableBatch processes the batch. The options may be nil.
	AcceptTableBatch(context.Context, *TableBatch, *AcceptOptions) error
}

// orderedAdapter adapts the TableAcceptor interface to MultiAcceptor,
// using a Watcher instance to drive table ordering.
type orderedAdapter struct {
	delegate TableAcceptor
	watchers Watchers
}

var _ MultiAcceptor = (*orderedAdapter)(nil)

// AcceptTableBatch implements TableAcceptor and will call the delegate
// with a last-on-wins behavior for each mutation key.
func (t *orderedAdapter) AcceptTableBatch(
	ctx context.Context, batch *TableBatch, options *AcceptOptions,
) error {
	return t.delegate.AcceptTableBatch(ctx, batch, options)
}

// AcceptTemporalBatch implements TemporalAcceptor and will delegate
// to AcceptMultiBatch.
func (t *orderedAdapter) AcceptTemporalBatch(
	ctx context.Context, batch *TemporalBatch, options *AcceptOptions,
) error {
	multi := &MultiBatch{
		Data:   []*TemporalBatch{batch},
		ByTime: map[hlc.Time]*TemporalBatch{batch.Time: batch},
	}
	return t.AcceptMultiBatch(ctx, multi, options)
}

// AcceptMultiBatch implements MultiAcceptor. It coalesces updates for
// all tables in the batch and iterates over the tables in dependency
// order.
func (t *orderedAdapter) AcceptMultiBatch(
	ctx context.Context, batch *MultiBatch, options *AcceptOptions,
) error {
	if batch.Count() == 0 {
		return nil
	}

	// Determine the final disposition of any given row. A row will be
	// present in exactly one of these maps.
	var commonSchema ident.Schema
	var deletes, updates ident.TableMap[map[string]Mutation]
	if err := batch.CopyInto(AccumulatorFunc(func(table ident.Table, mut Mutation) error {
		if commonSchema.Empty() {
			commonSchema = table.Schema()
		} else if !ident.Equal(commonSchema, table.Schema()) {
			return errors.Errorf("mixed-schema batches not currently supported: %s vs %s",
				commonSchema, table.Schema())
		}

		key := string(mut.Key)

		var setInTable, clearFromTable *ident.TableMap[map[string]Mutation]
		if mut.IsDelete() {
			setInTable = &deletes
			clearFromTable = &updates
		} else {
			setInTable = &updates
			clearFromTable = &deletes
		}

		setInMap, ok := setInTable.Get(table)
		if !ok {
			setInMap = make(map[string]Mutation)
			setInTable.Put(table, setInMap)
		}
		setInMap[key] = mut

		if clearFromMap, ok := clearFromTable.Get(table); ok {
			delete(clearFromMap, key)
			if len(clearFromMap) == 0 {
				clearFromTable.Delete(table)
			}
		}
		return nil
	})); err != nil {
		return err
	}

	w, err := t.watchers.Get(commonSchema)
	if err != nil {
		return err
	}
	order := w.Get().Order

	// Apply deletes in reverse table order.
	if deletes.Len() > 0 {
		for i := len(order) - 1; i >= 0; i-- {
			tablesInLevel := order[i]
			for _, table := range tablesInLevel {
				if mutsByKey, ok := deletes.Get(table); ok {
					nextBatch := &TableBatch{
						Table: table,
						Data:  make([]Mutation, 0, len(mutsByKey)),
					}
					for _, mut := range mutsByKey {
						nextBatch.Data = append(nextBatch.Data, mut)
					}
					if err := t.AcceptTableBatch(ctx, nextBatch, options); err != nil {
						return errors.Wrap(err, table.String())
					}
				}
			}
		}
	}

	// Apply updates in table order.
	if updates.Len() > 0 {
		for _, tablesInLevel := range order {
			for _, table := range tablesInLevel {
				if mutsByKey, ok := updates.Get(table); ok {
					nextBatch := &TableBatch{
						Table: table,
						Data:  make([]Mutation, 0, len(mutsByKey)),
					}
					for _, mut := range mutsByKey {
						nextBatch.Data = append(nextBatch.Data, mut)
					}
					if err := t.AcceptTableBatch(ctx, nextBatch, options); err != nil {
						return errors.Wrap(err, table.String())
					}
				}
			}
		}
	}

	return nil
}
