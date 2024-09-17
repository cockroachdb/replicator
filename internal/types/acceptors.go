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
	"runtime"
	"slices"
	"strings"

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
// in the table-dependency order as determined by the schema watcher.
//
// See also [UnorderedAcceptorFrom].
func OrderedAcceptorFrom(acc TableAcceptor, watchers Watchers) MultiAcceptor {
	switch t := acc.(type) {
	case *orderedAdapter:
		return t
	case *unorderedAdapter:
		// Avoid silly delegation.
		return &orderedAdapter{t.TableAcceptor, watchers}
	default:
		return &orderedAdapter{acc, watchers}
	}
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
// order. TemporalBatches that contain deletions will be emitted as
// though they were a separate batch, and the deletions applied in
// reverse dependency order.
func (t *orderedAdapter) AcceptMultiBatch(
	ctx context.Context, batch *MultiBatch, opts *AcceptOptions,
) error {
	// Peek to find the current schema.
	var commonSchema ident.Schema
	_ = batch.CopyInto(AccumulatorFunc(func(table ident.Table, _ Mutation) error {
		commonSchema = table.Schema()
		// Just break the loop.
		return context.Canceled
	}))

	// No mutations.
	if commonSchema.Empty() {
		return nil
	}
	watcher, err := t.watchers.Get(commonSchema)
	if err != nil {
		return err
	}
	w := watcher.Get()

	if !slices.IsSortedFunc(batch.Data, func(a, b *TemporalBatch) int {
		return hlc.Compare(a.Time, b.Time)
	}) {
		runtime.Breakpoint()
	}
	// This assumes that updates are more common than deletes. We want
	// to accumulate as many updates as possible, until we see a
	// deletion. If we have a deletion, we want to flush the
	// accumulator, apply the deletions, and then continue accumulating
	// updates.
	var pendingUpdates ident.TableMap[[]Mutation]
	for _, temporal := range batch.Data {

		// These are per-timestamp. The updates will be folded into
		// pendingUpdates at the bottom of the loop.
		var currentDeletes, currentUpdates ident.TableMap[[]Mutation]
		// Segment mutations by type and table. Ignoring return since no
		// error is returned from the callback.
		_ = temporal.Data.Range(func(table ident.Table, tableBatch *TableBatch) error {
			for _, mut := range tableBatch.Data {
				if mut.IsDelete() {
					currentDeletes.Put(table, append(currentDeletes.GetZero(table), mut))
				} else {
					currentUpdates.Put(table, append(currentUpdates.GetZero(table), mut))
				}
			}
			return nil
		})

		// If we have any deletes in this batch, we want to flush
		// all preceding updates, then the deletions.
		if currentDeletes.Len() > 0 {
			if err := t.flush(ctx, "update", w.Entire.Order, &pendingUpdates, opts); err != nil {
				return err
			}
			if err := t.flush(ctx, "delete", w.Entire.ReverseOrder, &currentDeletes, opts); err != nil {
				return err
			}
		}

		// Continue accumulating updates.
		_ = currentUpdates.Range(func(table ident.Table, muts []Mutation) error {
			pendingUpdates.Put(table, append(pendingUpdates.GetZero(table), muts...))
			return nil
		})
	}

	// Final flush. In cases where there are no deletions, this should
	// be the only time a flush happens. All deletes will have been
	// flushed in the loop above.
	return t.flush(ctx, "update", w.Entire.Order, &pendingUpdates, opts)
}

// Flush an accumulator table. This method guarantees that the
// accumulator is empty or that an error has been returned.
func (t *orderedAdapter) flush(
	ctx context.Context,
	kind string,
	order []ident.Table,
	acc *ident.TableMap[[]Mutation],
	opts *AcceptOptions,
) error {
	for _, table := range order {
		if muts, ok := acc.Get(table); ok {
			nextBatch := &TableBatch{Table: table, Data: muts}
			if err := t.AcceptTableBatch(ctx, nextBatch, opts); err != nil {
				return errors.Wrap(err, table.String())
			}
			acc.Delete(table)
		}
	}
	// We flushed all updates, so we're done.
	if acc.Len() == 0 {
		return nil
	}
	// There are leftover tables in the batch for which we do not
	// have a defined order.
	var sb strings.Builder
	_ = acc.Range(func(table ident.Table, _ []Mutation) error {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(table.Canonical().Raw())
		return nil
	})
	return errors.Errorf("%s sent to unknown tables: %s", kind, sb.String())
}

// UnorderedAcceptorFrom adapts a [TableAcceptor] to the [MultiAcceptor]
// interface. Table data will be delivered in an arbitrary order.
func UnorderedAcceptorFrom(acc TableAcceptor) MultiAcceptor {
	switch t := acc.(type) {
	case *orderedAdapter:
		// Prevent silly delegation.
		return t
	case *unorderedAdapter:
		// Prevent double-wrapping.
		return t
	default:
		return &unorderedAdapter{t}
	}
}

type unorderedAdapter struct {
	TableAcceptor
}

var _ MultiAcceptor = (*unorderedAdapter)(nil)

func (u *unorderedAdapter) AcceptMultiBatch(
	ctx context.Context, batch *MultiBatch, opts *AcceptOptions,
) error {
	for _, temp := range batch.Data {
		if err := u.AcceptTemporalBatch(ctx, temp, opts); err != nil {
			return err
		}
	}
	return nil
}

func (u *unorderedAdapter) AcceptTemporalBatch(
	ctx context.Context, batch *TemporalBatch, opts *AcceptOptions,
) error {
	if err := batch.Data.Range(func(table ident.Table, tableBatch *TableBatch) error {
		err := u.AcceptTableBatch(ctx, tableBatch, opts)
		return errors.Wrapf(err, "%s @ %s", table.Raw(), batch.Time)
	}); err != nil {
		return err
	}
	return nil
}
