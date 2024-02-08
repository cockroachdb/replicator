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

// This file defines various aggregations of mutations that translate
// into table-level work, a single transaction, or multiple source
// transactions. The "batch" noun is used since "transaction" generally
// refers to some activity taking place in a database.

import (
	"sort"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// The Batch interface is implemented by the various Batch types in this
// package.
type Batch[B any] interface {
	// Accumulate the mutation and ensure that batch invariants are
	// maintained.
	Accumulate(table ident.Table, mut Mutation) error

	// Count returns the number of mutations contained in the Batch.
	Count() int

	// Copy returns a deep copy of the Batch.
	Copy() B

	// Empty returns a copy of the Batch, but with no enclosed
	// mutations. This is useful when wanting to transform or filter a
	// batch.
	Empty() B
}

// A MultiBatch is a time-ordered collection of per-table data to apply.
// This represents the broadest scope of applying data, as it covers
// both time- and table-space and likely represents any number of
// source database transactions.
//
// A well-formed MultiBatch will have its Data field sorted by time.
// MultiBatch implements [sort.Interface] to make this simple.
type MultiBatch struct {
	noCopy
	ByTime map[hlc.Time]*TemporalBatch // Time-based indexing.
	Data   []*TemporalBatch            // Time-ordered indexed.
}

var (
	_ Batch[*MultiBatch] = (*MultiBatch)(nil)
	_ sort.Interface     = (*MultiBatch)(nil)
)

// Accumulate adds a mutation to the MultiBatch while maintaining the
// order invariants.
func (b *MultiBatch) Accumulate(table ident.Table, mut Mutation) error {
	if table.Empty() {
		return errors.New("empty table")
	}
	if mut.Time == hlc.Zero() {
		return errors.New("mutation has zero time")
	}
	// Append to existing batch. This will not affect the order elements
	// within the Data field, so we can just return.
	if existing, ok := b.ByTime[mut.Time]; ok {
		return existing.Accumulate(table, mut)
	}
	// If we create a new batch, we need to insert it in sorted order.
	batch := &TemporalBatch{
		Time: mut.Time,
	}
	if err := batch.Accumulate(table, mut); err != nil {
		return err
	}
	if b.ByTime == nil {
		b.ByTime = make(map[hlc.Time]*TemporalBatch)
	}
	b.ByTime[mut.Time] = batch
	b.Data = append(b.Data, batch)
	if len(b.Data) > 1 {
		sort.Sort(b)
	}
	return nil
}

// Copy returns a deep copy of the MultiBatch.
func (b *MultiBatch) Copy() *MultiBatch {
	ret := &MultiBatch{
		Data: make([]*TemporalBatch, len(b.Data)),
	}
	for idx, batch := range b.Data {
		ret.Data[idx] = batch.Copy()
	}
	return ret
}

// Count returns the number of enclosed mutations.
func (b *MultiBatch) Count() int {
	ret := 0
	for _, sub := range b.Data {
		ret += sub.Count()
	}
	return ret
}

// Empty returns an empty MultiBatch.
func (b *MultiBatch) Empty() *MultiBatch {
	return &MultiBatch{}
}

// Len implements [sort.Interface].
func (b *MultiBatch) Len() int {
	return len(b.Data)
}

// Less implements [sort.Interface].
func (b *MultiBatch) Less(i, j int) bool {
	return hlc.Compare(b.Data[i].Time, b.Data[j].Time) < 0
}

// Swap implements [sort.Interface].
func (b *MultiBatch) Swap(i, j int) {
	b.Data[i], b.Data[j] = b.Data[j], b.Data[i]
}

// A TableBatch contains mutations destined for a single table at a
// single timestamp. This likely corresponds to some part of a larger
// transaction.
type TableBatch struct {
	Data  []Mutation
	Table ident.Table
	Time  hlc.Time
}

var _ Batch[*TableBatch] = (*TableBatch)(nil)

// Accumulate a mutation.
func (b *TableBatch) Accumulate(table ident.Table, mut Mutation) error {
	if hlc.Compare(b.Time, mut.Time) != 0 {
		return errors.Errorf("mutation time (%s) does not equal batch time (%s)", mut.Time, b.Time)
	}
	if !ident.Equal(b.Table, table) {
		return errors.Errorf("mutation table (%s) does not equal batch table (%s)", table, b.Table)
	}
	b.Data = append(b.Data, mut)
	return nil
}

// Copy returns a deep copy of the batch.
func (b *TableBatch) Copy() *TableBatch {
	ret := *b
	ret.Data = append([]Mutation(nil), b.Data...)
	return &ret
}

// Count returns the number of enclosed mutations.
func (b *TableBatch) Count() int {
	return len(b.Data)
}

// Empty returns a TableBatch with the original metadata, but no data.
func (b *TableBatch) Empty() *TableBatch {
	ret := *b
	ret.Data = nil
	return &ret
}

// A TemporalBatch holds mutations for some number of tables that all
// occur at the same time. This likely corresponds to a single source
// transaction.
type TemporalBatch struct {
	noCopy
	Time hlc.Time
	Data ident.TableMap[*TableBatch]
}

var _ Batch[*TemporalBatch] = (*TemporalBatch)(nil)

// Accumulate adds a mutation for the given table to the batch.
func (b *TemporalBatch) Accumulate(table ident.Table, mut Mutation) error {
	if hlc.Compare(b.Time, mut.Time) != 0 {
		return errors.Errorf("mutation time (%s) does not equal batch time (%s)", b.Time, mut.Time)
	}
	batch, ok := b.Data.Get(table)
	if !ok {
		batch = &TableBatch{
			Table: table,
			Time:  b.Time,
		}
		b.Data.Put(table, batch)
	}
	return batch.Accumulate(table, mut)
}

// Copy returns a deep copy of the TemporalBatch.
func (b *TemporalBatch) Copy() *TemporalBatch {
	ret := &TemporalBatch{
		Time: b.Time,
	}
	_ = b.Data.Range(func(tbl ident.Table, b *TableBatch) error {
		ret.Data.Put(tbl, b.Copy())
		return nil
	})
	return ret
}

// Count returns the number of enclosed mutations.
func (b *TemporalBatch) Count() int {
	ret := 0
	_ = b.Data.Range(func(_ ident.Table, tbl *TableBatch) error {
		ret += tbl.Count()
		return nil
	})
	return ret
}

// Empty returns an empty TemporalBatch at the same time.
func (b *TemporalBatch) Empty() *TemporalBatch {
	return &TemporalBatch{
		Time: b.Time,
	}
}
