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
	"bytes"
	"context"
	"encoding/json"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/merge"
	"github.com/pkg/errors"
)

// Rekey reencodes the [types.Mutation.Key] field as mutations are
// processed, based on the destination table. This is primarily to
// support the pglogical REPLICA IDENTITIY FULL option, but it could be
// used for any other case where the replication key does not actually
// preserve replication identity.
type Rekey struct {
	watchers types.Watchers
}

var _ sequencer.Shim = (*Rekey)(nil)

// MultiAcceptor returns a rekeying facade around the acceptor.
func (r *Rekey) MultiAcceptor(acceptor types.MultiAcceptor) types.MultiAcceptor {
	return &rekeyAcceptor{
		base: base{
			multiAcceptor:    acceptor,
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		Rekey: r,
	}
}

// TableAcceptor returns a rekeying facade around the delegate.
func (r *Rekey) TableAcceptor(acceptor types.TableAcceptor) types.TableAcceptor {
	return &rekeyAcceptor{
		base: base{
			tableAcceptor: acceptor,
		},
		Rekey: r,
	}
}

// TemporalAcceptor returns a marking facade around the delegate.
func (r *Rekey) TemporalAcceptor(acceptor types.TemporalAcceptor) types.TemporalAcceptor {
	return &rekeyAcceptor{
		base: base{
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		Rekey: r,
	}
}

// Wrap implements [sequencer.Shim].
func (r *Rekey) Wrap(_ *stopper.Context, delegate sequencer.Sequencer) (sequencer.Sequencer, error) {
	return &rekeyShim{r, delegate}, nil
}

type rekeyShim struct {
	*Rekey
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*rekeyShim)(nil)

// Start will inject the facade at the top of the stack.
func (r *rekeyShim) Start(ctx *stopper.Context, opts *sequencer.StartOptions) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	acc, stat, err := r.delegate.Start(ctx, opts)
	return r.MultiAcceptor(acc), stat, err
}

type rekeyAcceptor struct {
	*Rekey
	base
}

var _ types.MultiAcceptor = (*rekeyAcceptor)(nil)

func (r *rekeyAcceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	if r.multiAcceptor == nil {
		return errors.New("not a MultiAcceptor")
	}
	next, err := r.multi(batch)
	if err != nil {
		return err
	}
	return r.multiAcceptor.AcceptMultiBatch(ctx, next, opts)
}

func (r *rekeyAcceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if r.tableAcceptor == nil {
		return errors.New("not a TableAcceptor")
	}
	next, err := r.table(batch)
	if err != nil {
		return err
	}
	return r.tableAcceptor.AcceptTableBatch(ctx, next, opts)
}

func (r *rekeyAcceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	if r.temporalAcceptor == nil {
		return errors.New("not a TemporalAcceptor")
	}
	next, err := r.temporal(batch)
	if err != nil {
		return err
	}
	return r.temporalAcceptor.AcceptTemporalBatch(ctx, next, opts)
}

func (r *rekeyAcceptor) multi(batch *types.MultiBatch) (*types.MultiBatch, error) {
	ret := batch.Empty()
	ret.ByTime = make(map[hlc.Time]*types.TemporalBatch, len(batch.Data))
	ret.Data = make([]*types.TemporalBatch, len(batch.Data))

	for idx, temp := range batch.Data {
		next, err := r.temporal(temp)
		if err != nil {
			return nil, err
		}
		ret.ByTime[next.Time] = next
		ret.Data[idx] = next
	}
	return ret, nil
}

func (r *rekeyAcceptor) table(batch *types.TableBatch) (*types.TableBatch, error) {
	watcher, err := r.watchers.Get(batch.Table.Schema())
	if err != nil {
		return nil, err
	}
	colData, ok := watcher.Get().Columns.Get(batch.Table)
	if !ok {
		return nil, errors.Errorf("unknown table %s", batch.Table)
	}
	bagSpec := &merge.BagSpec{Columns: colData}

	ret := batch.Empty()
	return ret, batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		// Shortest useful json we could decode is {"a":0}
		if mut.IsDelete() && len(mut.Data) < 7 {
			return ret.Accumulate(table, mut)
		}
		bag := merge.NewBag(bagSpec)
		dec := json.NewDecoder(bytes.NewReader(mut.Data))
		dec.UseNumber()
		if err := dec.Decode(&bag); err != nil {
			return errors.WithStack(err)
		}
		var jsKey []any
		for _, col := range colData {
			if !col.Primary {
				break
			}
			keyVal, ok := bag.Get(col.Name)
			if !ok {
				return errors.Errorf("could not rekey mutation; missing PK column %s", col.Name)
			}
			jsKey = append(jsKey, keyVal)
		}
		var err error
		mut.Key, err = json.Marshal(jsKey)
		if err != nil {
			return errors.WithStack(err)
		}
		ret.Data = append(ret.Data, mut)
		return nil
	}))
}

func (r *rekeyAcceptor) temporal(batch *types.TemporalBatch) (*types.TemporalBatch, error) {
	ret := batch.Empty()
	for table, tableBatch := range batch.Data.All() {
		var err error
		tableBatch, err = r.table(tableBatch)
		if err != nil {
			return nil, err
		}
		ret.Data.Put(table, tableBatch)
	}
	return ret, nil
}
