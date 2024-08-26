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
		// Avoid accidental double-wrapping.
		if t.watchers == watchers {
			return t
		}
		// This would be very unexpected.
		return &orderedAdapter{acc, watchers}
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
// order.
func (t *orderedAdapter) AcceptMultiBatch(
	ctx context.Context, batch *MultiBatch, options *AcceptOptions,
) error {
	if batch.Count() == 0 {
		return nil
	}
	// Coalesce all data by destination table.
	var commonSchema ident.Schema
	var mutsByTable ident.TableMap[[]Mutation]
	for _, temporal := range batch.Data {
		if err := temporal.Data.Range(func(tbl ident.Table, tblBatch *TableBatch) error {
			if commonSchema.Empty() {
				commonSchema = tbl.Schema()
			} else if !ident.Equal(commonSchema, tbl.Schema()) {
				return errors.Errorf("mixed-schema batches not currently supported: %s vs %s", commonSchema, tbl.Schema())
			}

			mutsByTable.Put(tbl, append(mutsByTable.GetZero(tbl), tblBatch.Data...))
			return nil
		}); err != nil {
			return errors.Wrap(err, temporal.Time.String())
		}
	}

	w, err := t.watchers.Get(commonSchema)
	if err != nil {
		return err
	}

	// Iterate over data in dependency order.
	for _, level := range w.Get().Order {
		for _, table := range level {
			if muts, ok := mutsByTable.Get(table); ok {
				nextBatch := &TableBatch{Table: table, Data: muts}
				if err := t.AcceptTableBatch(ctx, nextBatch, options); err != nil {
					return errors.Wrap(err, table.String())
				}
				mutsByTable.Delete(table)
			}
		}
	}

	// Ensure that we haven't ignored any tables in the input.
	if mutsByTable.Len() > 0 {
		var unknown strings.Builder
		// Callback returns nil.
		_ = mutsByTable.Range(func(table ident.Table, _ []Mutation) error {
			if unknown.Len() > 0 {
				unknown.WriteString(", ")
			}
			unknown.WriteString(table.Raw())
			return nil
		})
		return errors.Errorf(
			"unable to determine apply order for unknown tables: %s",
			unknown.String())
	}

	return nil
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
